var _= require('underscore'),
    _deep= require('./deep'),
    async= require('async');

const  _compare= function (x, y)
       {
           if (x===y)
             return 0;

           return x > y ? 1 : -1; 
       },
       _ignoreNotFound= function (done,$id,$pos,query)
       {
            return function (err)
            {
                if (err.code=='notfound')
                {
                  query.identity.set($id,$pos,undefined);
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
                if (!query.count) console.log('client side limit'.red);
                items= items.slice(0,query.limit-query.$returned);
                query.limited= true;
            }

            if (query.skip&&!query.skipped)
            {
                console.log('client side skip'.red);
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
                      })
                   },
       _filter= function (items,query)
       {
            var fieldNames= Object.keys(query.$filter);

            if (fieldNames.length)
            {

                console.log('filtering results in memory'.red);

                fieldNames.forEach(function (fieldName)
                {
                    var field= query.$filter[fieldName];
                    delete query.$filter[fieldName];
                    query.$filtered.push(fieldName);

                    items= _.filter(items,_operators[field.op](fieldName,field.values)); 
                });          

            }

            return items;
       },
       _sort= function (items,query)
       {
            if (query.orderby&&!query.sorted)
            {
               console.log('sorting results in memory'.red);

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
       _modifiers= function (items,query)
       {
           items= _filter(items,query);
           items= _sort(items,query);
           items= _limit(items,query);

           return items;
       },
       _refineItems= function (trigger,items,query)
       {
                if (query.canLimit())
                  items= _limit(items,query);

                var _load= function (key,proot,done)
                    {
                           var item= items[key];

                           query.identity.set(item.$id,0,item);

                           async.forEach(Object.keys(item),
                           function (field,done)
                           {
                               if (field.indexOf('$$$')==0)
                               {
                                 var attr= field.substring(3),
                                     $id= item[field];

                                 query.identity.get($id,'_',function (items)
                                 {
                                     if (items)
                                     {
                                         item[attr]= items;
                                         done();
                                     }
                                     else
                                         query.table.find({ $id: $id },
                                                          query.toprojection(proot[attr]),
                                                          query.identity)
                                         .results(function (values)
                                         {
                                            item[attr]= values;
                                            query.identity.set($id,'_',values);
                                            done();
                                         })
                                         .error(_ignoreNotFound(done,$id,'_',query));
                                 });
                               }
                               else
                               if (field.indexOf('$$')==0)
                               {
                                 var attr= field.substring(2),
                                     $id= item[field], $pos= 0;

                                 query.identity.get($id,$pos,function (loaded)
                                 {
                                     if (loaded)
                                     {
                                         item[attr]= loaded; 
                                         done();
                                     }
                                     else
                                         query.table.findOne({ $id: $id },
                                                             query.toprojection(proot[attr]),
                                                             query.identity)
                                         .result(function (value)
                                         {
                                            item[attr]= value; 
                                            query.identity.set($id,$pos,value);
                                            done();
                                         })
                                         .error(_ignoreNotFound(done,$id,$pos,query));
                                 });
                               }
                               else
                               if (field=='$ref')
                               {
                                  var $id= item.$ref, $pos= 0;

                                  query.identity.get($id,$pos,function (loaded)
                                  {
                                     if (loaded)
                                     {
                                         item= loaded; 
                                         done();
                                     }
                                     else
                                         query.table.findOne({ $id: $id },
                                                               query.toprojection(proot),
                                                               query.identity)
                                         .result(function (loaded)
                                         {
                                            item= loaded;
                                            query.identity.set($id,$pos,loaded);
                                            done();
                                         })
                                         .error(_ignoreNotFound(done,$id,$pos,query));
                                  });
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
                          query.identity.set(item.$id,0,item);

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
                               items[key]= item.$ref ? item : _.omit(item,query.projection.exclude);

                               wrapped(key,
                               function (err)
                               {
                                  if (err) done(err);
                                  else
                                  {
                                    var item= items[key];
                                    items[key]= item.$ref ? _.omit(item,query.projection.exclude) : item;
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
                                    var item= items[key];
                                    _deep.clone(item,function (clone)
                                    {
                                       item.$old= clone;
                                       done();
                                    });
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
       };

module.exports= function (dyn, query)
{
    var promises= ['results','end'];

    if (query.count) promises.push('count');

    var refiner= dyn.promise(promises), ended, _end= refiner.trigger.end, _results, _items;

    refiner.trigger.end= _.wrap(refiner.trigger.end,function (trigger)
    {
        // do nothing when finder ends
    });

    refiner.trigger.results= _.wrap(refiner.trigger.results,function (trigger,items)
    {
        _results= trigger;

        process.nextTick(function () // so we can call _end
        {
            _refineItems(function (items)
            {
                 if (items.next&&!query.limited)
                   items.next();
                 else
                   ended= true;

                 delete items.next;

                 query.$returned+= items.length;

                 if (query.count)
                 {
                     process.stdout.write(('\r'+query.$returned).yellow);

                     if (query.canCount())
                       refiner.trigger.count(query.$returned);
                 }
                 else
                 if (items.length>0)
                   _results(items);

                 if (ended) 
                 {
                    if (query.$returned==0)
                      _results([]);

                    _end();
                 }
            },
            items,query);
        });
    });
    
    return refiner;
};
