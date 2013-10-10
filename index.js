var dyno= require('./lib/dyn.js'),
    diff = require('deep-diff').diff,
    uuid= require('node-uuid').v4,
    _= require('underscore'),
    async= require('async'); 

var _parser= require('./lib/parser'), 
    _finder= require('./lib/finder'),
    _refiner= require('./lib/refiner'),
    _index= require('./lib/indexer'),
    _deep= require('./lib/deep'),
    _modify= require('./lib/capacity');

const _traverse= function (o, fn)
      {
         Object.keys(o).forEach(function (i)
         {
             fn.apply(null,[i,o[i],o]);
             if (typeof (o[i])=='object')
               _traverse(o[i],fn);
         });
      };

module.exports= function (opts,cb)
{
   
   if (!cb)
   {
     cb= opts;
     opts= {};
   }

   opts= opts || {};

   var dyn= dyno(opts.dynamo),
       finder= _finder(dyn),
       parser= _parser(dyn),
       db= { _dyn: dyn },
       _alias= function (table)
       {
          return (opts.tables || {} )[table] || table;
       };

   db.cleanup= function (obj)
   {
      var p= dyn.promise('clean');

      _deep.clone(obj,function (clone)
      {
          _deep.traverse(clone,
          function (key, value, obj)
          {
             if (key.indexOf('$')==0&&key!='$id')
               delete obj[key]; 
          });

          p.trigger.clean(clone);
      });

      return p;
   };


   var configureTable= function (table)
       {
            table.find= function (cond,projection,identity)
            {
                var p, modifiers= {};

                p= dyn.promise(['results','count','end']);

                process.nextTick(function ()
                {
       //            buildQuery.apply(modifiers,args);
                   parser
                   .parse(table,modifiers,cond,projection,identity)
                   .parsed(function (query)
                   {
                       refiner= _refiner(dyn,query),
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
                var p, args= arguments;

                p= dyn.promise('result','notfound');

                table.find.apply(table,args).limit(1).results(function (items)
                {
                     if (items.length==0)
                       p.trigger.notfound();
                     else
                       p.trigger.result(items[0]); 
                })
                .error(p.trigger.error);

                return p;
            };

            table.save= function (_obj)
            {
                var objs= Array.isArray(_obj) ? _obj : [_obj],
                    p= dyn.promise([],'updatedsinceread'), found= false;

                async.forEach(objs,
                function (obj,done)
                {
                    var gops= {},
                        ops= gops[table._dynamo.TableName]= [],
                        _hashrange= function (obj)
                        {
                            obj.$id= obj.$id || uuid();
                            obj.$pos= obj.$pos || 0;
                            obj.$version= (obj.$version || 0)+1;
                        },
                        _index= function (obj)
                        {
                             if (!obj.$ref)
                             table.indexes.forEach(function (index)
                             {
                                var iops= index.update(obj) || {};

                                _.keys(iops).forEach(function (table)
                                {
                                   var tops= gops[table]= gops[table] || []; 
                                   tops.push.apply(tops,_.collect(iops[table],function (op) { op.index= true; return op; }));
                                });
                             });
                        },
                        _save= function (obj)
                        {
                           var _keys= _.keys(obj),
                               _omit= ['$old'],
                               diffs= diff(obj.$old || {},_.omit(obj,'$old'));

                           if ((obj.$id&&_keys.length==1)||!diffs
                                ||(obj.$old || {$version: 0}).$version<obj.$version) return;

                           _hashrange(obj);
                           _index(obj);

                           _keys.forEach(function (key)
                           {
                                var type= typeof obj[key];

                                if (type=='object'&&key!='$old')
                                {
                                   var desc= obj[key];

                                   if (desc==null)
                                     delete obj[key];
                                   else
                                   if (Array.isArray(desc))
                                   {
                                       if (desc.length)
                                       {
                                           if (typeof desc[0]=='object')
                                           {
                                               var $id= obj['$$$'+key]= obj['$$$'+key] || uuid();

                                               desc.forEach(function (val, pos)
                                               {
                                                  if (val.$id&&val.$id!=$id)
                                                  {
                                                     _save(val);
                                                     _save({ $id: $id, $pos: pos, $ref: val.$id+'$:$'+val.$pos });
                                                  }
                                                  else
                                                  {
                                                     val.$id= $id;
                                                     val.$pos= pos;
                                                     _save(val);
                                                  }
                                               });

                                               _omit.push(key);
                                           }
                                       }
                                       else
                                       {
                                          var $id= obj['$$$'+key];

                                          if ($id&&obj.$old[key].length)
                                            obj.$old[key].forEach(function (item)
                                            {
                                               ops.push({ op: 'del', item: { $id: $id, $pos: item.$pos } });
                                            });

                                          delete obj['$$$'+key];
                                          delete obj[key];
                                       }
                                   }
                                   else
                                   {
                                       _save(desc);
                                       obj['$$'+key]= desc.$id+'$:$'+desc.$pos;
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

                           ops.push({ op: 'put', item: obj, omit: _omit });
                        },
                        _mput= function (gops,done)
                        {
                           async.forEach(_.keys(gops),
                           function (_table,done)
                           {
                              var tops= gops[_table];

                              async.forEach(tops,
                              function (op,done)
                              {
                                 var tab= dyn.table(_table),
                                     obj= op.item;
                                   
                                 if (op.index)
                                   tab.hash('$hash',obj.$hash)
                                      .range('$range',obj.$range);
                                 else
                                   tab.hash('$id',obj.$id)
                                      .range('$pos',obj.$pos);

                                 if (op.op=='put')
                                     tab.put(_.omit(obj,op.omit),
                                      function ()
                                      {
                                         _deep.clone(_.omit(obj,'$old'),function (clone)
                                         {
                                             obj.$old= clone;
                                             done();
                                         });
                                      },
                                      { expected: obj.$old ? { $version: obj.$old.$version } : undefined })
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


                    _save(obj);

                    _.keys(gops).forEach(function (table)
                    {
                        if (gops[table].length==0)
                          delete gops[table];
                        else
                          found= true;
                    });

                    if (found)
                      _mput(gops,done);
                    else
                        process.nextTick(done);

                },
                function (err)
                {
                      if (err)
                      {
                        if (err.code='notfound')
                          p.trigger.updatedsinceread();
                        else
                          p.trigger.error(err);
                      }
                      else
                          p.trigger.success();
                });

                return p;
            };

            table.ensureIndex= function (fields)
            {
                  var p= dyn.promise();

                  process.nextTick(function ()
                  {
                      var index= _index(dyn,table,fields);

                      if (index)
                        index.ensure(function (err)
                        {
                             if (err)
                               p.trigger.error(err);
                             else
                             {
                               table.indexes.push(index);
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
                var p= dyn.promise(),
                    cursor= table.find(filter,table.indexes.length ? undefined : { $id: 1, $pos: 1 }),
                    _deleteItem= function (obj,done)
                    {
                          async.parallel([
                          function (done)
                          {
                              async.forEach(table.indexes,
                              function (index,done)
                              {
                                   index.remove(obj,done);
                              },done);
                          },
                          function (done)
                          {
                              dyn.table(table._dynamo.TableName)
                                 .hash('$id',obj.$id)
                                 .range('$pos',obj.$pos)
                                 .delete(done)
                                 .error(done);
                          }],
                          done);
                    };

                cursor.results(function (items)
                {
                    async.forEach(items,_deleteItem,
                    function (err)
                    {
                       if (err)
                         cursor.trigger.error(err); 
                       else
                       if (items.next)
                         items.next();
                    });
                })
                .error(p.trigger.error)
                .end(p.trigger.success);

                return p;
            };

            table.update= function (query,update)
            {
                var p= dyn.promise(),
                    cursor= table.find(query),
                    _updateItem= function (item,done)
                    {
                       if (update.$set)
                         table.save(_.extend(item,update.$set))
                              .success(done)
                              .error(done); 
                       else
                       if (update.$unset)
                         table.save(_.omit(item,_.keys(update.$unset)))
                              .success(done)
                              .error(done); 
                       else
                         done(new Error('unknown update type')); 
                    },
                    _updateItems= function (items)
                    {
                       async.forEach(items,_updateItem,
                       function (err)
                       {
                         if (err)
                           cursor.trigger.error(err);
                       }); 
                    };


                cursor
                     .results(_updateItems)
                     .error(p.trigger.error)
                     .end(p.trigger.success);

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
                    console.log('This may take a while...'.yellow);

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

                                if (hash.AttributeName&&hash.AttributeName=='$id'&&range&&range.AttributeName=='$pos')
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

        console.log('This may take a while...'.yellow);

        dyn.table(name)
           .hash('$id','S')
           .range('$pos','N')
           .create(function check()
           {
              dyn.table(name)
                 .hash('$id','xx')
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

   configureTables(cb);

};
