var dyno= require('./lib/dyn.js'),
    diff = require('deep-diff').diff,
    uuid= require('node-uuid').v4,
    _= require('underscore'),
    cclone= require('circularclone'),
    async= require('async'); 

var _parser= require('./lib/parser'), 
    _finder= require('./lib/finder'),
    _refiner= require('./lib/refiner'),
    _index= require('./lib/indexer'),
    _modify= require('./lib/capacity'),
    _backup= require('./lib/backup');

const _nu= function (v)
      {
         return typeof v!='undefined';
      }, 
      _arr= function (v)
      {
         return Array.isArray(v) ? v : [v];
      },
      _isobject= function (v)
      {
          return typeof v=='object'&&!Array.isArray(v);
      },
      _isobjectarr= function (v)
      {
          return typeof v=='object'&&Array.isArray(v)&&v.length>0&&typeof v[0]=='object';
      },
      _collect= function (consume)
      {
          return function (cons) 
          { 
                var c;

                if (cons.table)
                {
                    if (!(c=consume[cons.table]))
                      c= consume[cons.table]= { read: 0, write: 0 };

                    c.read+= cons.read;
                    c.write+= cons.write;
                }
                else
                _.keys(cons).forEach(function (table)
                {
                    var c, tcons= cons[table];

                    if (!(c=consume[table]))
                      c= consume[table]= { read: 0, write: 0 };

                    c.read+= tcons.read;
                    c.write+= tcons.write;
                }); 
          };
      };

var dyngo= module.exports= function (opts,cb)
{
   var defaults= { hints: true }; 

   if (!cb)
   {
     cb= opts;
     opts= defaults;
   }

   opts= opts || defaults;
   opts= _.defaults(opts,defaults);

   var dyn= dyno(opts.dynamo,_.extend(opts.tx || {},{ txTable: opts.txTable })),
       finder= _finder(dyn),
       parser= _parser(dyn,opts),
       backup= _backup(dyn,opts),
       db= _.extend({ _dyn: dyn },opts.tx,{ txTable: opts.txTable }),
       _alias= function (table)
       {
          return (opts.tables || {} )[table] || table;
       },
       _mput= function (gops,done,isCreate,consumed)
       {
           async.forEach(_.keys(gops),
           function (_table,done)
           {
              var tops= gops[_table];

              async.forEachSeries(tops, // forEachSeries: when deleting elements from array i need deletes of old item _pos done before new item _pos put
              function (op,done)
              {
                 var tab= dyn.table(_table),
                     obj= op.item;
                   
                 if (op.index)
                   tab.hash('_hash',obj._hash)
                      .range('_range',obj._range);
                 else
                   tab.hash('_id',obj._id)
                      .range('_pos',obj._pos);

                 if (op.op=='put')
                     tab.put(_.omit(obj,op.omit),
                      function ()
                      {
                         obj._old= cclone(_.omit(obj,'_old'));
                         done();
                      },
                      { expected: obj._old&&_nu(obj._old._rev) ? { _rev: obj._old._rev } : undefined,
                        exists: isCreate ? false : undefined })
                      .consumed(consumed)
                      .error(done);
                 else
                 if (op.op=='del')
                     tab.delete(done)
                        .error(done);
                 else
                   done(new Error('unknown update type:'+op.op));
              },
              done);
           },
           done);
       };

   db.cleanup= function (obj)
   {
      var p= dyn.promise('clean');

      process.nextTick(function ()
      {
          p.trigger.clean(cclone(obj,function (key,value)
          {
             if (key.indexOf&&key.indexOf('_')==0&&key!='_id')
               return undefined; 
             else
               return value;
          }));
      });

      return p;
   };


   var configureTable= function (table)
       {
            table.ensuredIndexes= [];

            table.find= function (cond,projection,identity)
            {
                var p, modifiers= {}, table= this;

                modifiers.$consistent= !!table.$consistent;

                p= dyn.promise(['results','count','end'],null,'consumed');

                process.nextTick(function ()
                {
                   parser
                   .parse(table,modifiers,cond,projection,identity)
                   .parsed(function (query)
                   {
                       refiner= _refiner(dyn,query,db),
                       cursor= finder.find(query);
                       cursor.chain(refiner);
                       refiner.chain(p);
                   })
                   .error(p.trigger.error);
                });

                p.sort= function (o)
                {
                  modifiers.orderby= o; 
                  return p;
                };

                p.limit= function (n)
                {
                  modifiers.limit= n; 
                  return p;
                };

                p.window= function (n)
                {
                  modifiers.window= n; 
                  return p;
                };

                p.skip= function (n)
                {
                  modifiers.skip= n; 
                  return p;
                };

                p.noderef= function ()
                {
                  modifiers.noderef= true; 
                  return p;
                };

                var origCount= _.bind(p.count,p);

                p.count= function (fn)
                {
                  if (fn)
                     origCount(fn);
                  else 
                     modifiers.count= true; 

                  return p;
                };

                return p;
            };

            table.findOne= function ()
            {
                var p, args= arguments, table= this;

                p= dyn.promise('result','notfound','consumed');

                table.find.apply(table,args).limit(1).results(function (items)
                {
                     if (items.length==0)
                       p.trigger.notfound();
                     else
                       p.trigger.result(items[0]); 
                })
                .consumed(p.trigger.consumed)
                .error(p.trigger.error);

                return p;
            };

            table.consistent= function ()
            {
                return _.extend({ $consistent: true }, table);
            };

            table.save= function (_obj,isCreate)
            {
                var objs= _obj ? _arr(_obj) : [],
                    consume= {},
                    p= dyn.promise(null,'updatedsinceread','consumed'), found= false;

                process.nextTick(function ()
                {

                    async.forEach(objs,
                    function (obj,done)
                    {
                        var gops= {},
                            ops= gops[table._dynamo.TableName]= [],
                            _hashrange= function (obj)
                            {
                                obj._id= obj._id || uuid();
                                obj._pos= obj._pos || 0;
                                obj._rev= (obj._rev || 0)+1;
                                obj._table= table._dynamo.TableName,
                                obj._refs= [];
                            },
                            _index= function (obj)
                            {
                                 table.indexes.forEach(function (index)
                                 {
                                    var iops= index.update(obj,'put') || {};

                                    _.keys(iops).forEach(function (table)
                                    {
                                       var tops= gops[table]= gops[table] || []; 
                                       tops.push.apply(tops,_.collect(iops[table],function (op) { op.index= true; return op; }));
                                       tops.index= true;
                                    });
                                 });
                            },
                            _remove= function (item)
                            {
                                ops.push({ op: 'del', item: { _id: item._id, _pos: item._pos } });

                                table.indexes.forEach(function (index)
                                {
                                    var iops= index.update(item,'del') || {};

                                    _.keys(iops).forEach(function (table)
                                    {
                                       var tops= gops[table]= gops[table] || []; 
                                       tops.push.apply(tops,_.collect(iops[table],function (op) { op.index= true; return op; }));
                                    });
                                });
                            },
                            _save= function (obj,isCreate,isAggregate)
                            {
                               var _keys= _.keys(obj),
                                   _omit= ['_old','_table'],
                                   diffs= diff(obj._old || {},
                                               obj,
                                               function (path,key) { return key=='_old'; });

                               if (!diffs || diffs.length==0
                                    || (obj._old || {_rev: 0})._rev<obj._rev
                                  ) return;

                               if (obj._table&&obj._table!=table._dynamo.TableName)
                               {
                                  db[obj._table].save(obj);
                                  return;
                               }

                               _hashrange(obj);

                               _keys.forEach(function (key)
                               {
                                    if (key.indexOf('___')==0)
                                    {
                                      if (!_isobjectarr(obj[key.substring(3)]))
                                      {
                                          _omit.push(key);
                                          return;
                                      }
                                    }
                                    else
                                    if (key.indexOf('__')==0)
                                    {
                                      if (!_isobject(obj[key.substring(2)]))
                                      {
                                          _omit.push(key);
                                          return;
                                      }
                                    }
                                    
                                    var type= typeof obj[key];

                                    if (type=='object'&&!_.contains(['_old','_refs'],key))
                                    {
                                       var desc= obj[key];

                                       if (desc==null)
                                         _omit.push(key);
                                       else
                                       if (desc instanceof Date)
                                       { /* let dyn convert */ }
                                       else
                                       if (_.keys(desc).length==0)
                                         _omit.push(key);
                                       else
                                       if (Array.isArray(desc))
                                       {
                                           if (desc.length)
                                           {
                                               if (typeof desc[0]=='object')
                                               {
                                                   var _id= obj['___'+key]= obj['___'+key] || uuid();

                                                   if (obj._old)
                                                   {
                                                       var old= obj._old[key];

                                                       if (old&&old.length>desc.length)
                                                         old.forEach(function (oitem,idx)
                                                         {
                                                            if (oitem._id==_id)
                                                            {
                                                                if (!_.findWhere(desc,{ _pos: oitem._pos }))
                                                                  _remove(oitem);
                                                            }
                                                            else
                                                            {
                                                                var elem= _.findWhere(desc,{ _id: oitem._id, _pos: oitem._pos });

                                                                if (!elem||elem!=desc[idx])
                                                                  _remove({ _id: _id, _pos: idx, _ref: dyn.ref(oitem) });         
                                                            }
                                                         });
                                                   }

                                                   desc.forEach(function (val, pos)
                                                   {
                                                      if (val._id&&val._id!=_id)
                                                      {
                                                         _save(val);
                                                         _save({ _id: _id, _pos: pos, _ref: dyn.ref(val) });
                                                         obj._refs.push(val._id);
                                                      }
                                                      else
                                                      {
                                                         val._id= _id;

                                                         if (!isNaN(val._pos)&&val._pos!=pos)
                                                         {
                                                           delete val['_old'];
                                                           delete val['_rev'];
                                                           _remove(val);
                                                         }

                                                         val._pos= pos;
                                                         _save(val);
                                                      }
                                                   });

                                                   _omit.push(key);
                                               }
                                           }
                                           else
                                           {
                                              var _id= obj['___'+key];

                                              if (_id&&obj._old[key].length)
                                                obj._old[key].forEach(_remove);

                                              _omit.push(key);
                                              _omit.push('___'+key);
                                           }
                                       }
                                       else
                                       {
                                           _save(desc);
                                           obj['__'+key]= dyn.ref(desc);
                                           obj._refs.push(desc._id);
                                           _omit.push(key);
                                       }
                                    } 
                                    else
                                    if (type=='string'&&!obj[key])
                                      _omit.push(key);
                                    else
                                    if (type=='number'&&isNaN(obj[key]))
                                      _omit.push(key);
                               });

                               if (!obj._refs.length) 
                                 delete obj['_refs'];
                               else
                                 obj._refs= _.uniq(obj._refs);

                               _index(obj); // index after _ fields are set so they are indexable too

                               var op= { op: 'put', item: obj, omit: _omit, isCreate: isCreate };

                               if (isAggregate)
                                 ops.unshift(op); // let the aggregate op came first of "contained" objects, so that the aggrgate version protects the rest
                               else
                                 ops.push(op);
                            };


                        _save(obj,isCreate,true);

                        _.keys(gops).forEach(function (table)
                        {
                            if (gops[table].length==0)
                              delete gops[table];
                            else
                              found= true;
                        });

                        if (found)
                          _mput(gops,done,isCreate,_collect(consume));
                        else
                          process.nextTick(done);

                    },
                    function (err)
                    {
                          p.trigger.consumed(consume);

                          if (err)
                          {
                            if (err.code=='notfound')
                              p.trigger.updatedsinceread();
                            else
                              p.trigger.error(err);
                          }
                          else
                              p.trigger.success();
                    });

                });

                return p;
            };

            table.create= function (obj)
            {
                var p= dyn.promise(null,'exists','consumed');

                table.save(obj,true)
                     .success(p.trigger.success)
                     .consumed(p.trigger.consumed)
                     .error(function (err)
                {
                   if (err.code=='found')
                     p.trigger.exists();
                   else
                     p.trigger.error(err);
                });

                return p;
            };

            table.enableIndex= function (fields)
            {
               var index= _index(dyn,table,fields,opts);
               table.indexes.push(index);
               table.ensuredIndexes.push(fields);
            };

            table.ensureIndex= function (fields)
            {
                  var p= dyn.promise();

                  process.nextTick(function ()
                  {
                      var index= _index(dyn,table,fields,opts);

                      if (index)
                        index.ensure(function (err)
                        {
                             if (err)
                               p.trigger.error(err);
                             else
                             {
                               table.indexes.push(index);
                               table.ensuredIndexes.push(fields);
                               p.trigger.success();
                             }
                        });
                      else
                        p.trigger.error(new Error('no known index type can index those fields'));
                  });

                  return p;
            };

            table.remove= function (filter)
            {
                var p= dyn.promise(null,null,'consumed'),
                    consume= {},
                    _consumed= function (cons)
                    {
                       consume.read+= cons.read;
                       consume.write+= cons.write;
                    },
                    _error= function (err)
                    {
                       p.trigger.consumed(consume);
                       p.trigger.error(err);
                    },
                    _success= function ()
                    {
                       p.trigger.consumed(consume);
                       p.trigger.success();
                    },
                    sync= dyn.syncResults(function (err)
                    {
                        if (err)
                          _error(err);
                        else
                          _success();
                    }),
                    cursor= table.find(filter,table.indexes.length ? undefined : { _id: 1, _pos: 1 }),
                    _deleteItem= function (obj,done)
                    {
                          async.parallel([
                          function (done)
                          {
                              async.forEach(table.indexes,
                              function (index,done)
                              {
                                   index.remove(obj).success(done).error(done).consumed(_collect(consume));
                              },done);
                          },
                          function (done)
                          {
                              dyn.table(table._dynamo.TableName)
                                 .hash('_id',obj._id)
                                 .range('_pos',obj._pos)
                                 .delete(done)
                                 .consumed(_collect(consume))
                                 .error(done);
                          }],
                          done);
                    };

                if (table.indexes.length==0)
                  cursor= cursor.noderef();

                cursor.results(sync.results(function (items,done)
                {
                    async.forEach(items,_deleteItem,done);
                }))
                .consumed(_consumed)
                .error(_error)
                .end(sync.end);

                return p;
            };

            table.update= function (query,update)
            {
                var p= dyn.promise(null,['updatedsinceread'],'consumed'),
                    cursor= table.consistent().find(query),
                    consume= {},
                    _consumed= function (cons)
                    {
                        _.keys(cons).forEach(function (table)
                        {
                            var c, tcons= cons[table];

                            if (!(c=consume[table]))
                              c= consume[table]= { read: 0, write: 0 };

                            c.read+= tcons.read;
                            c.write+= tcons.write;
                        }); 
                    },
                    _error= function (err)
                    {
                       p.trigger.consumed(consume);
                       p.trigger.error(err);
                    },
                    _success= function ()
                    {
                       p.trigger.consumed(consume);
                       p.trigger.success();
                    },
                    sync= dyn.syncResults(function (err)
                    {
                        if (err)
                          _error(err);
                        else
                          _success();
                    }),
                    _updateItem= function (item,done)
                    {
                       var fields= {},
                           _index= function (obj,done)
                           {
                                 var gops= {};

                                 table.indexes.forEach(function (index)
                                 {
                                    var iops= index.update(obj,'put') || {};

                                    _.keys(iops).forEach(function (table)
                                    {
                                       var tops= gops[table]= gops[table] || []; 
                                       tops.push.apply(tops,_.collect(iops[table],function (op) { op.index= true; return op; }));
                                       tops.index= true;
                                    });
                                 });

                                 _mput(gops,done,false,_collect(consume));
                           };

                       if (update.$set)
                         _.keys(update.$set).forEach(function (name)
                         {
                             var value= update.$set[name];

                             if (_isobject(value))
                             {
                                 if (value._id)
                                 {
                                     fields['_'+name]= { action: 'DELETE' };
                                     fields['__'+name]= { action: 'PUT', value: dyn.ref(value) };
                                     fields['___'+name]= { action: 'DELETE' };
                                     delete item[name];
                                     delete item['___'+name];
                                 }
                                 else
                                     done(new Error('cannot set new objects yet, use table.save')); 
                             }
                             else
                             if (_isobjectarr(value))
                               done(new Error('cannot set object arrays yet, use table.save')); 
                             else
                             {
                                 fields[name]= { action: 'PUT', value: update.$set[name] };
                                 item[name]= update.$set[name];
                                 fields['__'+name]= { action: 'DELETE' };
                                 fields['___'+name]= { action: 'DELETE' };
                                 delete item['__'+name];
                                 delete item['___'+name];
                             }
                         });

                       if (update.$unset)
                         _.keys(update.$unset).forEach(function (name)
                         {
                             fields[name]= { action: 'DELETE' };
                             fields['__'+name]= { action: 'DELETE' };
                             fields['___'+name]= { action: 'DELETE' };
                             delete item[name];
                             delete item['__'+name];
                             delete item['___'+name];
                         });

                       if (update.$inc)
                         _.keys(update.$inc).forEach(function (name)
                         {
                             fields[name]= { action: 'ADD', value: update.$inc[name] };
                             item[name]++;
                         });

                       if (update.$addToSet)
                         _.keys(update.$addToSet).forEach(function (name)
                         {
                             var value= _arr(update.$addToSet[name]);
                             fields[name]= { action: 'ADD', value: value };
                             item[name].push.apply(item[name],value)
                         });

                       if (update.$rmFromSet)
                         _.keys(update.$rmFromSet).forEach(function (name)
                         {
                             var value= _arr(update.$rmFromSet[name]);
                             fields[name]= { action: 'DELETE', value: value };
                             item[name]= _.difference(item[name],value);
                         });

                       if (_.keys(fields).length)
                       {
                         fields._rev= { action: 'ADD', value: 1 };

                         async.waterfall
                         ([function (done)
                          {
                             dyn.table(item._table)
                                .hash('_id',item._id)
                                .range('_pos',item._pos)
                                .updateItem({ update: fields,
                                            expected: item._old&&_nu(item._old._rev) ? { _rev: item._old._rev } : undefined },
                                 function() { done(); })
                                .consumed(_collect(consume))
                                .error(function (err)
                                {
                                    if (err.code=='notfound')
                                      p.trigger.updatedsinceread(item);
                                    else   
                                      done(err);
                                });
                          },
                          function (done)
                          {
                              _index(item,done);
                          }],
                          done);
                       }
                       else
                         done(new Error('unknown update type')); 
                    },
                    _updateItems= function (items,done)
                    {
                       async.forEach(items,_updateItem,done);
                    };


                cursor
                     .results(sync.results(_updateItems))
                     .consumed(_consumed)
                     .error(_error)
                     .end(sync.end);

                return p;
            };

            table.modify= function (read,write)
            {
                return _modify(dyn,table._dynamo.TableName,read,write);
            };

            table.drop= function ()
            {
                var p= dyn.promise(),
                    _success= function ()
                    {
                      delete db[_alias(table._dynamo.TableName)];
                      p.trigger.success();
                    },
                    _check= function ()
                    {
                          dyn.describeTable(table._dynamo.TableName,
                          function (err,data)
                          {
                              if (err)
                              {
                                  if (err.code=='ResourceNotFoundException')
                                    _success();
                                  else
                                    p.trigger.error(err);
                              }
                              else
                                setTimeout(_check,5000);
                          });
                    };
        
                async.forEach(table.indexes,
                function (index,done)
                {
                    index.drop(done);
                },
                function (err)
                {
                    if (opts.hints) console.log('This may take a while...'.yellow);

                    dyn.deleteTable(table._dynamo.TableName,function (err)
                    {
                        if (err)
                        {
                           if (err.code=='ResourceNotFoundException')
                             _success();
                           else
                             p.trigger.error(err);
                        }
                        else
                           setTimeout(_check,5000);
                    });
                });

                return p;
            };

            table.backup= backup.backup(table._dynamo.TableName);

            table.restore= backup.restore(table._dynamo.TableName);

            return table;
       },
       configureTables= function (cb)
       {
            var configure= function (tables)
                {
                    async.forEach(Object.keys(tables),
                    function (table,done)
                    {
                          dyn.describeTable(table,function (err,data)
                          {
                              if (!err)
                              {
                                var hash= _.findWhere(data.Table.KeySchema,{ KeyType: 'HASH' }),
                                    range= _.findWhere(data.Table.KeySchema,{ KeyType: 'RANGE' });

                                if (hash&&hash.AttributeName&&hash.AttributeName=='_id'&&range&&range.AttributeName=='_pos')
                                  db[tables[table]]= configureTable({ _dynamo: data.Table, indexes: [] });
                              }

                              done(err);
                          });
                    },
                    function (err)
                    {
                       cb(err,err ? null : db);          
                    });
                };

             if (opts.tables)
               configure(opts.tables);
             else
               dyn.listTables(function (err,list)
               {
                   if (err)
                     cb(err);
                   else
                   {
                       var tables= {};
                       list.forEach(function (table) { tables[table]= table; });
                       configure(tables);
                   }
               });
       };

   db.createCollection= function (name)
   { 
      var p= dyn.promise(),
          _success= function ()
          {
              dyn.describeTable(name,function (err,data)
              {
                  if (!err)
                  {
                    db[name]= configureTable({ _dynamo: data.Table, indexes: [] });
                    p.trigger.success();
                  }
                  else
                    p.trigger.error(err);

              });
          };

        if (opts.hints) console.log('This may take a while...'.yellow);

        dyn.table(name)
           .hash('_id','S')
           .range('_pos','N')
           .create(function check()
           {
              dyn.table(name)
                 .hash('_id','xx')
                 .query(function ()
              {
                 _success();
              })
              .error(function (err)
              {
                 if (err.code=='ResourceNotFoundException')
                   setTimeout(check,5000);
                 else
                 if (err.code=='notfound')
                   _success();
                 else
                   p.trigger.error(err);
              });
           })
           .error(function (err)
           {
               if (err.code=='ResourceInUseException')
                 p.trigger.error(new Error('the collection exists'));
               else
                 p.trigger.error(err);
           });

      return p;
   };

   db.ensureTransactionTable= function (topts)
   { 
      topts= _.defaults(topts || {},{ name: 'dyngo-transaction-table' });

      var p= dyn.promise(),
          _success= function ()
          {
              dyn.describeTable(topts.name,function (err,data)
              {
                  if (!err)
                  {
                    db.txTable= { _dynamo: data.Table, indexes: [] };

                    db.txTable.modify= function (read,write) { return _modify(dyn,data.Table.TableName,read,write) };

                    db.txTable.drop= function ()
                    { 
                        var p= dyn.promise(),
                            _success= function ()
                            {
                              delete db.txTable;
                              p.trigger.success();
                            },
                            _check= function ()
                            {
                                  dyn.describeTable(data.Table.TableName,
                                  function (err,data)
                                  {
                                      if (err)
                                      {
                                          if (err.code=='ResourceNotFoundException')
                                            _success();
                                          else
                                            p.trigger.error(err);
                                      }
                                      else
                                        setTimeout(_check,5000);
                                  });
                            };
                
                         if (opts.hints) console.log('This may take a while...'.yellow);

                         dyn.deleteTable(data.Table.TableName,function (err)
                         {
                                if (err)
                                {
                                   if (err.code=='ResourceNotFoundException')
                                     _success();
                                   else
                                     p.trigger.error(err);
                                }
                                else
                                   setTimeout(_check,5000);
                         });

                         return p;
                    };

                    p.trigger.success();
                  }
                  else
                    p.trigger.error(err);
              });
          };

        if (opts.hints) console.log('This may take a while...'.yellow);

        dyn.table(topts.name)
           .hash('_id','S')
           .range('_item','S')
           .create(function check()
           {
              dyn.table(topts.name)
                 .hash('_id','xx')
                 .query(function ()
              {
                 _success();
              })
              .error(function (err)
              {
                 if (err.code=='ResourceNotFoundException')
                   setTimeout(check,5000);
                 else
                 if (err.code=='notfound')
                   _success();
                 else
                   p.trigger.error(err);
              });
           })
           .error(function (err)
           {
               if (err.code=='ResourceInUseException')
                 _success();
               else
                 p.trigger.error(err);
           });

      return p;
   };

   db.transaction= function (txOpts)
   {
         var p= dyn.promise('transaction',null,'consumed'),
             consume= {};

         process.nextTick(function ()
         {
             if (!db.txTable)
             {
               p.trigger.error(new Error('no transaction table defined'));
               return;
             }

             var tab= dyn.table(db.txTable._dynamo.TableName),
                 init= function (tx)
                 {               
                     dyn.table(db.txTable._dynamo.TableName)
                        .hash('_id',tx._id)
                        .range('_item','_')
                        .put(tx,function ()
                        {
                           var dopts= _.extend({ tx: tx, txTable: db.txTable },opts,txOpts);

                           dyngo(dopts,
                           function (err,tx)
                           {
                              if (err)
                              {  
                                p.trigger.error(err);
                                return;
                              }

                              _.filter(_.keys(tx),function (key) { return !!tx[key].find; })
                              .forEach(function (tableName)
                              {
                                  db[tableName].ensuredIndexes.forEach(tx[tableName].enableIndex); // use enableIndex (sync) do not ensure..
                              });

                              dopts.tx.transaction= _.bind(db.transaction,db);

                              tx.commit= function ()
                              {
                                  var p= dyn.promise('committed','rolledback','consumed'),
                                      consume= {},
                                      _commit= function (cb)
                                      {
                                          dyn.table(db.txTable._dynamo.TableName)
                                             .hash('_id',tx._id)
                                             .range('_item','_')
                                             .updateItem({ update: { state: { action: 'PUT', value: 'committed' } },
                                                         expected: { state: 'pending' } },
                                             function ()
                                             {
                                                tx.state= 'committed';
                                                cb();   
                                             })
                                             .consumed(_collect(consume))
                                             .error(function (err)
                                             {
                                                 if (err.code=='notfound')
                                                   p.trigger.rolledback(true);
                                                 else
                                                   p.trigger.error(err);
                                             });
                                      },
                                      _complete= function (cb)
                                      {
                                          var sync= dyn.syncResults(function (err)
                                          {
                                              if (err)
                                                p.trigger.error(err);
                                              else
                                                cb();
                                          });

                                          dyn.table(db.txTable._dynamo.TableName)
                                             .hash('_id',tx._id)
                                             .range('_item','target::','BEGINS_WITH')
                                             .query(sync.results(function (items,done)
                                              {
                                                 async.forEach(items,
                                                 function (item,done)
                                                 {
                                                     var _item= item._item.split('::'),
                                                         table= _item[1],
                                                         hash= { attr: _item[2], value: _item[3] },
                                                         range= { attr: _item[4], value: _item[4]=='_pos' ? +_item[5] : _item[5] };

                                                     if (item._txOp=='delete')
                                                         dyn.table(table)
                                                            .hash(hash.attr,hash.value)
                                                            .range(range.attr,range.value)
                                                            .delete(function () { done(); },{ expected: { _tx: tx._id } })
                                                            .consumed(_collect(consume))
                                                            .error(done);
                                                     else
                                                         dyn.table(table)
                                                            .hash(hash.attr,hash.value)
                                                            .range(range.attr,range.value)
                                                            .updateItem({ update: { _txTransient: { action: 'DELETE' },
                                                                                    _txApplied: { action: 'DELETE' },
                                                                                    _txDeleted: { action: 'DELETE' },
                                                                                    _txLocked: { action: 'DELETE' },
                                                                                    _tx: { action: 'DELETE' } } },
                                                            function () { done(); })
                                                            .consumed(_collect(consume))
                                                            .error(done);
                                                      
                                                 },
                                                 done);
                                              }),
                                              { attrs: ['_id','_item','_txOp'],
                                           consistent: true })
                                             .error(p.trigger.error)
                                             .consumed(_collect(consume))
                                             .end(sync.end);
                                      },
                                      _clean= function (cb)
                                      {
                                          dyn.table(db.txTable._dynamo.TableName)
                                             .hash('_id',tx._id)
                                             .range('_item','_')
                                             .updateItem({ update: { state: { action: 'PUT', value: 'completed' } },
                                                           expected: { state: 'committed' } },
                                             function ()
                                             {
                                                tx.state= 'completed';
                                                cb();   
                                             })
                                             .consumed(p.trigger.consumed)
                                             .error(p.trigger.error);
                                      },
                                      _committed= function ()
                                      {
                                          p.trigger.consumed(consume);
                                          p.trigger.committed();
                                      };

                                  if (tx.state=='pending')
                                    _commit(function ()
                                    {
                                          _complete(function ()
                                          {
                                               _clean(_committed);
                                          });
                                    });
                                  else
                                  if (tx.state=='committed')
                                    _complete(function ()
                                    {
                                         _clean(_committed);
                                    });
                                  else
                                    p.trigger.error(new Error("Invalid transaction state: "+tx.state));

                                  return p;
                              };

                              tx.rollback= function ()
                              {
                                  var p= dyn.promise('rolledback',null,'consumed'),
                                      consume= {},
                                      _rollback= function (cb)
                                      {
                                          var sync= dyn.syncResults(function (err)
                                              {
                                                  if (err)
                                                    p.trigger.error(err);
                                                  else
                                                      dyn.table(db.txTable._dynamo.TableName)
                                                         .hash('_id',tx._id)
                                                         .range('_item','_')
                                                         .updateItem({ update: { state: { action: 'PUT', value: 'rolledback' } },
                                                                     expected: { state: 'pending' } },
                                                         function ()
                                                         {
                                                            tx.state= 'rolledback';
                                                            cb();   
                                                         })
                                                         .consumed(_collect(consume))
                                                         .error(p.trigger.error);
                                              });

                                          dyn.table(db.txTable._dynamo.TableName)
                                             .hash('_id',tx._id)
                                             .range('_item','target::','BEGINS_WITH')
                                             .query(sync.results(function (items,done)
                                              {
                                                 async.forEach(items,
                                                 function (item,done)
                                                 {
                                                     var _item= item._item.split('::'),
                                                         table= _item[1],
                                                         hash= { attr: _item[2], value: _item[3] },
                                                         range= { attr: _item[4], value: _item[4]=='_pos' ? +_item[5] : _item[5] },
                                                         clean= function ()
                                                         {
                                                               dyn.table(table)
                                                                  .hash(hash.attr,hash.value)
                                                                  .range(range.attr,range.value)
                                                                  .updateItem({ update: { _txTransient: { action: 'DELETE' },
                                                                                          _txApplied: { action: 'DELETE' },
                                                                                          _txDeleted: { action: 'DELETE' },
                                                                                          _txLocked: { action: 'DELETE' },
                                                                                          _tx: { action: 'DELETE' } } },
                                                                   function () { done(); })
                                                                  .consumed(_collect(consume))
                                                                  .error(done);
                                                         };

                                                     if (_.contains(['put','updateItem'],item._txOp))
                                                       dyn.table(table)
                                                          .hash(hash.attr,hash.value)
                                                          .range(range.attr,range.value)
                                                          .get(function (item)
                                                           {
                                                               if (item._txTransient)
                                                                 dyn.table(table)
                                                                    .hash(hash.attr,hash.value)
                                                                    .range(range.attr,range.value)
                                                                    .delete(function () { done(); })
                                                                    .consumed(_collect(consume))
                                                                    .error(done); 
                                                               else
                                                                   dyn.table(db.txTable._dynamo.TableName)
                                                                      .hash('_id',tx._id)
                                                                      .range('_item',['copy',
                                                                                      table,
                                                                                      hash.attr,
                                                                                      hash.value,
                                                                                      range.attr,
                                                                                      range.value].join('::'))
                                                                      .get(function (copy)
                                                                       {
                                                                              copy= _.omit(copy,['_id',
                                                                                                 '_item',
                                                                                                 '_txLocked',
                                                                                                 '_txApplied',
                                                                                                 '_txDeleted',
                                                                                                 '_txTransient',
                                                                                                 '_tx']);

                                                                              copy[hash.attr]= hash.value;
                                                                              copy[range.attr]= range.value;

                                                                              dyn.table(table)
                                                                                 .hash(hash.attr,hash.value)
                                                                                 .range(range.attr,range.value)
                                                                                 .put(copy,function () { done(); })
                                                                                 .consumed(p.trigger.consumed)
                                                                                 .error(p.trigger.error); 
                                                                       })
                                                                       .consumed(_collect(consume))
                                                                       .error(done); 
                                                           })
                                                           .consumed(_collect(consume))
                                                           .error(done); 
                                                     else
                                                       clean();
                                                 },
                                                 done);
                                              }),
                                              { attrs: ['_id','_item','_txOp'],
                                           consistent: true })
                                             .error(p.trigger.error)
                                             .consumed(_collect(consume))
                                             .end(sync.end);
                                      };

                                  if (tx.state=='pending')
                                    _rollback(p.trigger.rolledback);
                                  else
                                    p.trigger.error(new Error("Invalid transaction state: "+tx.state));

                                  return p;
                              };

                              p.trigger.consumed(consume);
                              p.trigger.transaction(tx);
                           });
                        })
                        .consumed(_collect(consume))
                        .error(p.trigger.error);

                };

                if (typeof txOpts=='string')
                  tab.hash('_id',txOpts)
                     .range('_item','_')
                     .get(init,{ consistent: true })
                     .consumed(_collect(consume))
                     .error(p.trigger.error);
                else
                {
                     if (opts.tx)
                       p.trigger.error(new Error('cannot start a transaction within a transaction'));
                     else
                       init({ _id: uuid(), _item: '_', state: 'pending' });
                }
         });

         return p;
   };

   configureTables(cb);

};
