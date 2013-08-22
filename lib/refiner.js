var _= require('underscore'),
    _deep= require('./deep'),
    async= require('async');

const  _compare= function (x, y)
       {
           if (x===y)
             return 0;

           return x > y ? 1 : -1; 
       },
       _limit= function (items,query)
       {
            if (!query.limited)
            {
                items= query.limit&&items.length>query.limit ? items.slice(0,query.limit) : items;
                items= query.skip ? items.slice(query.skip) : items;
                query.limited= true;
            }

            return items;
       },
       _sort= function (items,query)
       {
            if (query.orderby&&!query.sorted)
            {
               console.log('sorting results'.red);

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
       };

module.exports= function (dyn, query)
{
    var refiner= dyn.promise('results');

    refiner.trigger.results= _.wrap(refiner.trigger.results,function (trigger,items)
    {
            if (!(query.orderby&&!query.sorted))
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
                                     .error(done);
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
                                     .error(done);
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
                                     .error(done);
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

            async.forEach(Object.keys(items),
            _refine,
            function (err)
            {
               if (err)
                 refiner.trigger.error(err);
               else
                 trigger(_limit(_sort(items,query),query));
            });
    });
    
    return refiner;
};
