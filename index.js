var dyno= require('./lib/dyn.js'),
    diff = require('deep-diff').diff,
    uuid= require('node-uuid').v4,
    _= require('underscore'),
    async= require('async'); 

const _traverse= function (o, fn)
      {
         Object.keys(o).forEach(function (i)
         {
             fn.apply(null,[i,o[i],o]);
             if (typeof (o[i])=='object')
               _traverse(o[i],fn);
         });
      },
      _oa = function(o, s) 
      {
            s = s.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
            s = s.replace(/^\./, '');           // strip a leading dot
            var a = s.split('.');
            while (a.length) {
                var n = a.shift();
                if (n in o) {
                    o = o[n];
                } else {
                    return;
                }
            }
            return o;
      },
      _deepclone= function (obj)
      {
         return JSON.parse(JSON.stringify(obj));
      },
      _index= function (dyn,table,fields)
      {
         var index= {},
             _fields= _.collect(Object.keys(fields),
                                function (fieldName)
                                { 
                                    var type= fields[fieldName];

                                    if (type.secondary) 
                                      return { name: fieldName, 
                                               type: type.type,
                                          secondary: true };
                                    else
                                      return { name: fieldName, 
                                               type: type };
                                }),
             primaryFieldNames= [],
             secondaryFieldNames= [];

         index.name= table._dynamo.TableName+'--'
                     +_.collect(_fields,
                                function (field) { return field.secondary ?
                                                          '__'+field.name : 
                                                          field.name; })
                       .join('-').replace(/\$/g,'_');

         _fields.forEach(function (field)
         {
             if (field.secondary)
               secondaryFieldNames.push(field.name);
             else
               primaryFieldNames.push(field.name);
         });
 
         index.create= function (done)
         {
                var _secondary= function ()
                {
                    var secondaryFields= _.filter(_fields,function (field) { return !!field.secondary; });
                    return _.collect(secondaryFields,
                           function (field)
                           {
                                return { name: field.name, 
                                         key: { name: field.name,
                                                type: field.type },
                                         projection: ['$id','$pos','$version'] };
                           });    
                };

                dyn.table(index.name)
                   .hash('$hash','S')
                   .range('$range','S')
                   .create(function indexItems()
                   {
                      dyn.table(index.name)
                         .hash('$hash','xx')
                         .query(function ()
                      {
                         done(new Error('Should not find anything!'));
                      })
                      .error(function (err)
                      {
                         if (err.code=='ResourceNotFoundException')
                           setTimeout(indexItems,5000);
                         else
                         if (err.code=='notfound')
                           table.find().results(function (items)
                           {
                               async.forEach(items,index.put,done);
                           })
                           .error(done);
                         else
                           done(err);
                      });
                   },{ secondary: _secondary() })
                   .error(function (err)
                   {
                       if (err.code=='ResourceInUseException')
                         setTimeout(function () { index.ensure(done) },5000);
                       else
                         done(err);
                   });
         };

         index.delete= function (done)
         {
             dyn.deleteTable(index.name,done);
         };

         index.exists= function (done)
         {
              dyn.table(index.name)
                 .hash('$hash','xx')
                 .query(function ()
              {
                 done(null,true);
              })
              .error(function (err)
              {
                  if (err.code=='ResourceNotFoundException')
                    done(null,false); 
                  else
                  if (err.code=='notfound')
                    done(null,true); 
                  else
                    done(err);
              });
         };

         index.rebuild= function (done)
         {
             index.delete(function (err)
             {
                 if (err)
                   done(err);
                 else
                   index.create(done);
             });
         };

         index.ensure= function (done)
         {
             index.exists(function (err, exists)
             { 
                if (err)
                  done(err);
                else
                if (exists)
                  done();
                else
                  index.create(done);
             });
         };

         index.makeHash= function (item)
         {
            return _.collect(primaryFieldNames,
                             function (fieldName) { return _oa(item,fieldName); }).join(':');
         };

         index.makeCondHash= function (cond)
         {
            return _.collect(primaryFieldNames,
                             function (fieldName) { return cond[fieldName]; }).join(':');
         };

         index.makeRange= function (item)
         {
            return item.$id+':'+item.$pos;
         };

         index.indexable= function (item)
         {
            return item&&!_.some(primaryFieldNames,
                           function (fieldName) { return !_oa(item,fieldName); });
         };

         index.usable= function (cond)
         {
            return cond&&!_.some(primaryFieldNames,
                           function (fieldName) { return !cond[fieldName]; });
         };

         index.makeElement= function (item)
         {
            return _.extend(_.pick(item,_.union(['$id','$pos','$version'],
                                                secondaryFieldNames)),
                            { '$hash': index.makeHash(item),
                              '$range': index.makeRange(item) });
         };

         index.put= function (item,done)
         {
             if (index.indexable(item))
             {
               var elem= index.makeElement(item);

               dyn.table(index.name)
                  .hash('$hash',elem.$hash)
                  .range('$range',elem.$range)
                  .put(elem,done)
                  .error(done);
             }
             else
               done(); 
         };

         index.update= function (item,done)
         {
              var iops= {},
                  ops= iops[index.name]= [];

              if (index.indexable(item))
              {
                  var elem= index.makeElement(item);

                  if (index.indexable(item.$old))
                  {
                     var old= index.makeElement(item.$old);

                     if (elem.$hash!=old.$hash
                       ||elem.$range!=old.$range)
                      ops.push({ op: 'del', item: _.pick(old,['$hash','$range']) });
                  }

                  ops.push({ op: 'put', item: elem });
              }

              if (ops.length)
                return iops;
              else
                return undefined;
         };

         index.remove= function (item,done)
         {
             if (index.indexable(item))
               dyn.table(index.name)
                  .hash('$hash',index.makeHash(item))
                  .range('$range',index.makeRange(item))
                  .delete(done)
                  .error(function (err)
               {
                  if (err.code=='notfound')
                    done();
                  else
                    done(err);
               });
             else
               done();
         };

         return index; 
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
       db= {};

   db.cleanup= function (obj)
   {
      var clone= _deepclone(obj);
      _traverse(clone, function (key, value, clone)
      {
         if (key.indexOf('$')==0&&key!='$id')
           delete clone[key]; 
      });

      return clone;
   };


   var configureTable= function (table)
       {
            var buildQuery= function (cond,projection)
            {
               var filter= {},
                   attrs,
                   attrsToRemove= [], 
                   modifiers= this,
                   cache= {},
                   get= function (hash,pos,done)
                   {
                       var found;
                       var _hash= cache[hash]= cache[hash] || [];

                       if (!(found=cache[hash][pos]))
                       {
                         _hash[pos]= true; 
                         dyn.table(table._dynamo.TableName)
                            .hash('$id',hash)
                            .range('$pos',pos)
                            .get(function (value)
                            {
                               var _hash= cache[hash]= cache[hash] || [];
                               _hash[pos]= value; 
                               console.log('got',hash);
                               done(null,value); 
                            })
                            .error(done);
                       }
                       else
                       if (found===true)
                         setTimeout(get,100,hash,pos,done);
                       else
                         done(null,found);
                   },
                   load= function (item,__done)
                   {
                       console.log('load',item.$id,item.$ref);
                       var done= function (err)
                       {
                            if (err)
                              __done(err);
                            else
                            {
                               attrsToRemove.forEach(function (attr)
                               {
                                   if (item[attr])
                                     delete item[attr]; 
                               });

                               item.$old= _deepclone(item);
                               __done();
                            }
                       };

                       async.forEach(Object.keys(item),
                       function (field,done)
                       {
                           if (field.indexOf('$$$')==0)
                           {
                             var attr= field.substring(3);

                             if (!_.contains(attrsToRemove,attr))
                               dyn.table(table._dynamo.TableName)
                                  .hash('$id',item[field])
                                  .query(function (values)
                                  {
                                       item[attr]= values;

                                       async.forEach(values,load,done);
                                  })
                                  .error(done);
                             else
                             {
                                delete item[field];
                                done();
                             }
                           }
                           else
                           if (field.indexOf('$$')==0)
                           {
                             var attr= field.substring(2);

                             if (!_.contains(attrsToRemove,attr))
                                 get(item[field],0,function (err,value)
                                 {
                                     if (err) done(err);
                                     else
                                     {
                                         item[attr]= value;

                                         if (typeof value=='object')
                                           load(value,done);
                                         else
                                           done();
                                     }
                                 });
                             else
                             {
                                delete item[field];
                                done();
                             }
                           }
                           else
                           if (field=='$ref')
                             get(item.$ref,0,function (err,value)
                             {
                                 if (err) done(err);
                                 else
                                 {
                                     _.extend(item,value);
                                     delete item.$ref;
                                     load(item,done);
                                 }
                             });
                           else
                             done();
                       },
                       function (err)
                       {
                          done(err); 
                       });
                   };

               var scan= true, pk=false, index, indexFields, condFields= _.keys(cond || {});

               if (condFields.length==1&&condFields[0]=='$id')
                 pk= true;
               else
               if (cond)
                 table.indexes.every(function (ind)
                 {
                      if (ind.usable(cond))
                      {
                        index= ind;
                        scan= false;
                        return false;                        
                      }
                 });

               var p= modifiers.promise; 

               if (projection)
               {
                 attrs= ['$id'];

                 _.keys(projection).forEach(function (attr)
                 { 
                        if (attr.indexOf('$')!=0)
                        {
                            if (projection[attr]==1)
                            {
                              attrs.push(attr);
                              attrs.push('$$'+attr);
                              attrs.push('$$$'+attr);
                            }
                            else
                            if (projection[attr]==-1)
                              attrsToRemove.push(attr);
                            else
                              p.trigger.error(new Error('unknown projection value'));
                        }
                        else
                          p.trigger.error(new Error('invalid projection value'));
                 });

                 if (attrs.length==1)
                   attrs= undefined;
               }

               if (pk)
               {
                   console.log('PK on '+table._dynamo.TableName+' for '+JSON.stringify(cond,null,2));
                      dyn.table(table._dynamo.TableName)
                         .hash('$id',cond.$id)
                         .query(function (items)
                      {
                           async.forEach(items,function (item,done)
                           {
                              load(item,done);
                           },
                           function (err)
                           {
                              if (err)
                                p.trigger.error(err);
                              else
                              if (items.length)
                                p.trigger.results(items);
                              else
                                p.trigger.notfound();
                           });
                      },{ attrs: attrs })
                      .error(p.trigger.error); 
               }
               else
               if (scan) 
               {
                   if (cond)
                     Object.keys(cond).forEach(function (field)
                     {
                           filter[field]= { values: [ cond[field] ],
                                                op: 'EQ' };
                           
                     });

                   console.log('SCAN on '+table._dynamo.TableName+' for '+JSON.stringify(cond,null,2));
                   dyn.table(table._dynamo.TableName)
                      .scan(function (items)
                      {
                           async.forEach(items,function (item,done)
                           {
                              load(item,done);
                           },
                           function (err)
                           {
                              if (err)
                                p.trigger.error(err);
                              else
                              if (items.length)
                                p.trigger.results(items);
                              else
                                p.trigger.notfound();
                           });
                           //p.trigger.results(items);
                      },{ filter: filter, attrs: attrs })
                      .error(p.trigger.error);
               }
               else
               {
                  var hash= index.makeCondHash(cond),
                      tab= dyn.table(index.name)
                              .hash('$hash',hash),
                      sort;

                  console.log('Query INDEX ('+index.name+') HASH',hash);

                  if (modifiers.orderby)
                  Object.keys(modifiers.orderby).every(function (field)
                  {
                     sort= { field: field, asc: modifiers.orderby[field]>0 };
                     return false;
                  });

                  console.log('SORT',sort);

                  if (sort)
                    tab.index(sort.field);

                  tab.query(function (items)
                  {
                       var notfound= [];

                       async.forEach(items,function (item,done)
                       {
                          dyn.table(table._dynamo.TableName)
                             .hash('$id',item.$id)
                             .range('$pos',item.$pos)
                             .get(function (gitem)
                          {
                             load(_.extend(item,gitem),done);
                          },{ attrs: attrs })
                          .error(function (err)
                          {
                             if (err.code=='notfound')
                             {
                               notfound.push(item);
                               done();
                             }
                             else
                               done(err); 
                          });
                       },
                       function (err)
                       {
                          if (err)
                            p.trigger.error(err);
                          else
                          {
                            notfound.forEach(function (item) // dirty index
                            {
                               items= _.without(items,item);
                            });

                            if (items.length) 
                              p.trigger.results(items);
                            else
                              p.trigger.notfound();
                          }
                       });
                  },{ attrs: ['$id','$pos'], desc: sort&&!sort.asc })
                  .error(p.trigger.error);
               }
            };

            table.find= function ()
            {
                var p, orderby, args= arguments;

                p= dyn.promise('results','notfound');

                process.nextTick(function ()
                {
                   buildQuery.apply({ orderby: orderby, promise: p },args);
                });

                p.sort= function (o)
                {
                  orderby= o; 
                  return p;
                };

                return p;
            };

            table.findOne= function ()
            {
                var p, args= arguments;

                p= dyn.promise('result','notfound');

                table.find.apply(table,args).results(function (items)
                {
                     p.trigger.result(items[0]); 
                })
                .error(p.trigger.error);

                return p;
            };

            table.save= function (_obj)
            {
                var obj= JSON.parse(JSON.stringify(_obj)), 
                    gops= {},
                    ops= gops[table._dynamo.TableName]= [];

                var _hashrange= function (obj)
                    {
                        obj.$id= obj.$id || uuid();
                        obj.$pos= obj.$pos || 0;
                        obj.$version= (obj.$version || 0)+1;
                    },
                    _index= function (obj)
                    {
                         table.indexes.forEach(function (index)
                         {
                            var iops= index.update(obj) || {};

                            _.keys(iops).forEach(function (table)
                            {
                               var tops= gops[table]= gops[table] || []; 
                               tops.push.apply(tops,iops[table]);
                            });
                         });
                    },
                    _save= function (obj)
                    {
                       var _keys= _.keys(obj),
                           diffs= diff(obj.$old || {},_.omit(obj,'$old'));

                       console.log(diffs);

                       if ((obj.$id&&_keys.length==1)||!diffs) return;

                       _hashrange(obj);
                       _index(obj);

                       _keys.forEach(function (key)
                       {
                            var type= typeof obj[key];

                            if (type=='object'&&key!='$old')
                            {
                               var desc= obj[key];

                               if (Array.isArray(desc))
                               {
                                   if (desc.length&&typeof desc[0]=='object')
                                   {
                                       var $id= obj['$$$'+key]= obj['$$$'+key] || uuid();

                                       desc.forEach(function (val, pos)
                                       {
                                          if (val.$id&&val.$id!=$id)
                                          {
                                             _save(val);
                                             val.$ref= val.$id;
                                          }

                                          val.$id= $id;
                                          val.$pos= pos;
                                          _save(val);
                                       });

                                       delete obj[key];
                                   }
                               }
                               else
                               {
                                   _save(desc);
                                   obj['$$'+key]= desc.$id;
                                   delete obj[key];
                               }
                            } 
                            else
                            if (type=='string'&&!obj[key])
                              delete obj[key];
                       });

                       ops.push({ op: 'put', item: _.omit(obj,['$old']) });
                    };

                var p= dyn.promise();

                _save(obj);

                console.log(JSON.stringify(gops,null,2));

                dyn.mput(gops,p.trigger.success)
                   .error(p.trigger.error);

                return p;
            };

            table.ensureIndex= function (fields)
            {
                  var p= dyn.promise(),
                      index= _index(dyn,table,fields);

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

                  return p;
            };

            table.remove= function ()
            {
                var p= dyn.promise(),
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

                table.find.apply(table,arguments).results(function (items)
                {
                    async.forEach(items,_deleteItem,
                    function (err)
                    {
                       if (err)
                         p.trigger.error(err); 
                       else
                         p.trigger.success();
                    });
                })
                .error(p.trigger.error);

                return p;
            };

            table.update= function (query,update)
            {
                var p= dyn.promise(),
                    _updateItem= function (item,done)
                    {
                       if (update.$set)
                         table.save(_.extend(item,update.$set))
                              .success(done)
                              .error(done); 
                       else
                         done(new Error('unknown update type')); 
                    },
                    _updateItems= function (items)
                    {
                       async.forEach(items,_updateItem,p.should('success')); 
                    };


                table.find(query)
                     .results(_updateItems)
                     .error(p.trigger.error);

                return p;
            };

            table.drop= function ()
            {
                var p= dyn.promise(),
                    _alias= function (name)
                    {
                        var alias= name;

                        if (opts.tables)
                        _.keys(opts.tables).every(function (alias)
                        {
                              if (opts.tables[alias]==name)
                              {
                                alias= opts.tables[alias];
                                return false;
                              }
                        });
                        
                        return alias;
                    };

                dyn.deleteTable(table._dynamo.TableName,function (err)
                {
                    if (err)
                      p.trigger.error(err);
                    else
                    {
                      delete db[_alias(table._dynamo.TableName)];
                      p.trigger.success();
                    }
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
                                db[tables[table]]= configureTable({ _dynamo: data.Table, indexes: [] });

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

}
