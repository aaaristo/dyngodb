var _= require('underscore'),
    async= require('async');

const _deepclone= function (obj)
      {
         return JSON.parse(JSON.stringify(obj));
      };

module.exports= function (dyn, query)
{
    var refiner= dyn.promise('results','notfound');

    refiner.trigger.results= _.wrap(refiner.trigger.results,function (trigger,items)
    {
          if (items.length==0)
            refiner.trigger.notfound();
          else
          {
            items= query.limit&&items.length>query.limit ? items.slice(0,query.limit) : items;
            items= query.skip ? items.slice(query.skip) : items;

            var results= [], 
                _push= function (item)
                {
                   item.$old= _deepclone(item);
                   results.push(item);
                },
                _load= function (item,proot,done)
                {
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
                            done(null,item);
                       });
                },
                _refine= function (item, done)
                { 
                   _load(item, query.projection.root,
                   function (err,loaded)
                   {
                      if (err) done(err);
                      else
                        done(null,loaded);
                   });

                }; 

            if ((query.projection.exclude || []).length>0)
              _refine= _.wrap(_refine,function (wrapped,item,done)
                       {
                           wrapped(item.$ref ? item : _.omit(item,query.projection.exclude),
                           function (err,loaded)
                           {
                              if (err) done(err);
                              else
                              {
                                done(null,item.$ref ? _.omit(loaded,query.projection.exclude) : loaded);
                              }
                           });
                       });

             _refine= _.wrap(_refine,function (wrapped,item,done)
                      {
                           wrapped(item,
                           function (err,loaded)
                           {
                              if (err) done(err);
                              if (loaded)
                              {
                                 if (Array.isArray(loaded))
                                   loaded.forEach(_push);
                                 else
                                   _push(loaded);

                                 done();
                              }
                              else
                                done();
                           });
                      });

            async.forEach(items,_refine,
            function (err)
            {
               if (err)
                 refiner.trigger.error(err);
               else
               if (results.length>0)
                 trigger(results);
               else
                 refiner.trigger.notfound();
            });
          }
    });
    
    return refiner;
};
