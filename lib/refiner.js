var _= require('underscore'),
    cclone= require('circularclone'),
    diff = require('deep-diff').diff,
    async= require('async');

const  _compare= function (x, y)
       {
           if (x===y)
             return 0;

           return x > y ? 1 : -1; 
       },
       _collect= function (consume)
       {
          return function (cons) 
          { 
            _.keys(cons).forEach(function (table)
            {
                var c, tcons= cons[table];

                if (!(c=consume[table]))
                  c= consume[table]= { read: 0, write: 0 };

                c.read+= tcons.read;
                c.write+= tcons.write;
            }); 
          };
       },
       _ignoreNotFound= function (done,_id,_pos,query)
       {
            return function (err)
            {
                if (err.code=='notfound')
                {
                  query.identity.set(_id,_pos,undefined);
                  done();
                }
                else
                  done(err);
            };
       },
       _limit= function (items,query)
       {
            if (query.limit&&(items.length+query.$returned>query.limit)&&!query.limited)
            {
                if (!query.count&&query.opts.hints) console.log('client side limit'.red);
                items= items.slice(0,query.limit-query.$returned);
                query.limited= true;
            }

            if (query.skip&&!query.skipped)
            {
                if (query.opts.hints) console.log('client side skip'.red);

                items= items.slice(query.skip);
                query.skipped= true;
            }

            return items;
       },
       _oa = function(o, s) 
       {
             s = s.replace(/\[(\w+)\]/g, '.$1');
             s = s.replace(/^\./, '');
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
       _operator= function (cmp)
                  {
                      return function (fieldName,vals)
                             { 
                                  return function (item)
                                  {
                                      return cmp(_oa(item,fieldName),vals);
                                  };
                             };
                  },
       _operators= {
                      EQ: _operator(function (itemVal,vals)
                      {
                          return itemVal===vals[0];
                      }),

                      NE: _operator(function (itemVal,vals)
                      {
                          return itemVal!==vals[0];
                      }),

                      GT: _operator(function (itemVal,vals)
                      {
                          return itemVal>vals[0];
                      }),

                      GE: _operator(function (itemVal,vals)
                      {
                          return itemVal>=vals[0];
                      }),

                      LT: _operator(function (itemVal,vals)
                      {
                          return itemVal<vals[0];
                      }),

                      LE: _operator(function (itemVal,vals)
                      {
                          return itemVal<=vals[0];
                      }),

                      IN: _operator(function (itemVal,vals)
                      {
                         return _.contains(vals,itemVal);
                      }),

                      BEGINS_WITH: _operator(function (itemVal,vals)
                      {
                         return itemVal&&itemVal.indexOf(vals[0])==0;
                      }),

                      CONTAINS: _operator(function (itemVal,vals)
                      {
                         return itemVal&&itemVal.indexOf(vals[0])>-1;
                      }),

                      REGEXP: _operator(function (itemVal,vals)
                      {
                         return !!vals[0].exec(itemVal);
                      }),

                      BETWEEN: _operator(function (itemVal,vals)
                      {
                         return itemVal>=vals[0]&&itemVal<=vals[1];
                      }),

                      NULL: _operator(function (itemVal,vals)
                      {
                         return itemVal==undefined || itemVal==null;
                      }),

                      NOT_NULL: _operator(function (itemVal,vals)
                      {
                         return !(itemVal==undefined || itemVal==null);
                      }),
 
                      ALL: _operator(function (itemVal,vals)
                      {
                         return !_.difference(vals,itemVal).length;
                      })
                   },
       _filter= function (items,query)
       {

            var fieldNames= Object.keys(query.$filter), next= items.next;

            if (fieldNames.length)
            {
                if (query.opts.hints) console.log('client side filter'.red);

                fieldNames.forEach(function (fieldName)
                {
                    var field= query.$filter[fieldName];
                    delete query.$filter[fieldName];
                    query.$filtered.push(fieldName);
                    items= _.filter(items,_operators[field.op](fieldName,field.values)); 
                });          

            }

            items.next= next;

            return items;
       },
       _sort= function (items,query)
       {
            if (query.orderby&&!query.sorted)
            {
               if (query.opts.hints) console.log('client side sort'.red);

               var fields= query.$orderby;

               items.sort(function (x, y)
               {
                  var retval;

                  fields.some(function (field)
                  {
                      var fx= query.oa(x,field.name),
                          fy= query.oa(y,field.name);

                      if (fx!=fy)
                      {
                        retval= _compare(fx,fy)*field.dir;
                        return true;
                      }
                  });

                  return retval;
               }); 
            }

            return items;
       },
       _notfound= function (items)
       {
           var ret= _.filter(items,function (i) { return !!i._id; }); 
           
           ret.next= items.next;

           return ret;
       },
       _modifiers= function (items,query)
       {
           if (items.refine)
             items= items.refine(items);

           items= _notfound(items);
           items= _filter(items,query);
           items= _sort(items,query);
           items= _limit(items,query);

           return items;
       },
       _refineItems= function (dyn,trigger,items,query,db)
       {
                if (query.canLimit())
                  items= _limit(items,query);
               
                var p= dyn.promise(null,null,'consumed'),
                    _load= function (key,proot,done)
                    {
                           var item= items[key];

                           query.identity.set(item._id,0,item);

                           async.forEach(Object.keys(item),
                           function (field,done)
                           {
                               if (field.indexOf('___')==0)
                               {
                                 var attr= field.substring(3),
                                     _id= item[field];

                                 query.identity.get(_id,'_',function (items)
                                 {
                                     if (items)
                                     {
                                         item[attr]= items;
                                         done();
                                     }
                                     else
                                         query.table.find({ _id: _id },
                                                          query.toprojection(proot[attr]),
                                                          query.identity)
                                         .results(function (values)
                                         {
                                            item[attr]= values;
                                            query.identity.set(_id,'_',values);
                                            done();
                                         })
                                         .consumed(p.trigger.consumed)
                                         .error(_ignoreNotFound(done,_id,'_',query));
                                 });
                               }
                               else
                               if (field.indexOf('__')==0)
                               {
                                 var attr= field.substring(2),
                                     ptr= dyn.deref(item[field],query.table._dynamo.TableName);

                                 query.identity.get(ptr._id,ptr._pos,function (loaded)
                                 {
                                     if (loaded)
                                     {
                                         item[attr]= loaded; 
                                         done();
                                     }
                                     else
                                         db[ptr._table].findOne({ _id: ptr._id, _pos: ptr._pos },
                                                             query.toprojection(proot[attr]),
                                                             query.identity)
                                         .result(function (value)
                                         {
                                            item[attr]= value; 
                                            query.identity.set(ptr._id,ptr._pos,value);
                                            done();
                                         })
                                         .consumed(p.trigger.consumed)
                                         .error(_ignoreNotFound(done,ptr._id,ptr._pos,query));
                                 });
                               }
                               else
                               if (field=='_ref')
                               {
                                  var ptr;

                                  if (query.noderef)
                                  {
                                     if (!item._id)
                                       item= _.extend(item,dyn.deref(item._ref,query.table._dynamo.TableName));

                                     delete item['_ref'];
                                     done();
                                     return;
                                  }

                                  ptr= dyn.deref(item._ref,query.table._dynamo.TableName);

                                  var _diff= diff(query.toprojection(proot),{ _id: 1, _pos: 1 });

                                  if (_diff)
                                      query.identity.get(ptr._id,ptr._pos,function (loaded)
                                      {
                                         if (loaded)
                                         {
                                             item= loaded; 
                                             done();
                                         }
                                         else
                                             db[ptr._table].findOne({ _id: ptr._id, _pos: ptr._pos },
                                                                   query.toprojection(proot),
                                                                   query.identity)
                                             .result(function (loaded)
                                             {
                                                item= loaded;
                                                query.identity.set(ptr._id,ptr._pos,loaded);
                                                done();
                                             })
                                             .consumed(p.trigger.consumed)
                                             .error(_ignoreNotFound(done,ptr._id,ptr._pos,query));
                                      });
                                  else
                                  {
                                    item= { _id: ptr._id, _pos: ptr._pos };
                                    done();
                                  }
                               }
                               else
                                 done();
                           },
                           function (err)
                           {
                              if (err) done(err); 
                              else
                              {
                                items[key]= item;
                                done();
                              }
                           });
                    },
                    _refine= function (key, done)
                    { 
                       _load(key, query.projection.root,
                       function (err)
                       {
                          var item= items[key];
                          query.identity.set(item._id,0,item);

                          if (err)
                            done(err);
                          else
                            done();
                       });

                    }; 

                if ((query.projection.exclude || []).length>0)
                  _refine= _.wrap(_refine,function (wrapped,key,done)
                           {
                               var item= items[key];
                               items[key]= item._ref ? item : _.omit(item,query.projection.exclude);

                               wrapped(key,
                               function (err)
                               {
                                  if (err) done(err);
                                  else
                                  {
                                    var item= items[key];
                                    items[key]= item._ref ? _.omit(item,query.projection.exclude) : item;
                                    done();
                                  }
                               });
                           });

                 _refine= _.wrap(_refine,function (wrapped,key,done)
                          {
                               wrapped(key,
                               function (err)
                               {
                                  if (err) done(err)
                                  else
                                  {
                                    var item= items[key], cir= [],
                                        queue= [],
                                        set= function (known,clone,attr,filter)
                                             {
                                                var val= _.findWhere(known,filter); 

                                                if (val)
                                                  return val;
                                                else
                                                  queue.push(arguments);
                                             };

                                    item._table= query.table._dynamo.TableName;

                                    item._old= cclone(item,function (field,value,clone,node,origValue,known)
                                               {
                                                   if (typeof field!='string') return value;

                                                   if (field.indexOf('___')==0)
                                                   {
                                                      var attr= field.substring(3),
                                                          parts= value.split('$:$'),
                                                          _id= parts[0];

                                                      if (!clone[attr])
                                                        clone[attr]= set(known,clone,attr,{ _id: _id }); 
                                                      else
                                                      if (Array.isArray(clone[attr]))
                                                        clone[attr]._id= _id;
                                                   }
                                                   else
                                                   if (field.indexOf('__')==0)
                                                   {
                                                      var attr= field.substring(2),
                                                          parts= value.split('$:$'),
                                                          _id= parts[0];

                                                      if (!clone[attr])
                                                        clone[attr]= set(known,clone,attr,{ _id: _id, _pos: 0 }); 
                                                   }
                                                   else
                                                   if (field=='_ref')
                                                   {
                                                      var _id, _pos= 0;

                                                      if (typeof value=='string') 
                                                      {
                                                         var parts= value.split('$:$');
                                                         _id= parts[0];

                                                         if (parts[1])
                                                           _pos= +parts[1];
                                                      }
                                                      else
                                                      {
                                                        _id= value._id; 
                                                        _pos= value._pos; 
                                                      }

                                                      if (!clone[attr])
                                                        clone[attr]= set(known,clone,attr,{ _id: _id, _pos: _pos }); 
                                                   }
                                                   
                                                   return value;
                                               });

                                      queue.forEach(function (args)
                                      {
                                           set.apply(null,args);
                                      });

                                      done();
                                  }
                               });
                          });

                async.forEach(_.range(items.length),
                _refine,
                function (err)
                {
                   if (err)
                     refiner.trigger.error(err);
                   else
                     trigger(_modifiers(items,query));
                });

                return p;
       };

module.exports= function (dyn, query, db)
{
    var promises= ['results','end'];

    if (query.count) promises.push('count');

    var refiner= dyn.promise(promises,null,'consumed'), fended, ended, _end= refiner.trigger.end, _results, _items, consume= {}, _consumed= refiner.trigger.consumed,
        _stop= function ()
        {
           ended= true;
        };

    refiner.trigger.end= _.wrap(refiner.trigger.end,function (trigger)
    {
        fended= true;
    });

    refiner.trigger.consumed= _.wrap(refiner.trigger.consumed,function (trigger,cons)
    {
        var c;

        if (!(c=consume[cons.table]))
          c= consume[cons.table]= { read: 0, write: 0 };

        c.read+= cons.read;
        c.write+= cons.write;
    });

    refiner.trigger.results= _.wrap(refiner.trigger.results,function (trigger,items)
    {
        _results= trigger;

        if (!ended)
        process.nextTick(function () // so we can call _end
        {
            _refineItems(dyn,function (items)
            {
                 if (items.next&&!query.limited)
                   items.next();
                 
                 ended= query.limited || fended; 

                 delete items.next;

                 query.$returned+= items.length;

                 if (query.count)
                 {
                     process.stdout.write(('\r'+query.$returned).yellow);

                     if (query.canCount())
                     {
                       _consumed(consume);
                       refiner.trigger.count(query.$returned);
                     }
                 }
                 else
                 if (items.length>0)
                   _results(items,_stop);

                 if (ended) 
                 {
                    if (query.$returned==0)
                      _results([]);

                    _consumed(consume);
                    _end();
                 }
            },
            items,query,db).consumed(_collect(consume));
        });
    });
    
    return refiner;
};
