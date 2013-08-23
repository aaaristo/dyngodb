var async= require('async'),
    _= require('underscore'),
    colors= require('colors');

module.exports= function (dyn)
{
    var finder= {};

    finder.canFind= function (query)
    {
         query.table.indexes.some(function (ind)
         {
              if (ind.usable(query))
              {
                query.index= ind;
                return false;                        
              }
         });

         return !!query.index;
    };

    finder.find= function (query)
    {
       console.log(('Query INDEX ('+query.index.name+')').yellow);

       var p= dyn.promise(['results','count']),
           cursor= query.index.find(query);

       if (query.count)
           cursor.chain(p);
       else
           cursor.results(function (items)
           {
              var _get= function (key,done)
              {
                   var item= items[key];

                   dyn.table(query.table.name)
                      .hash('$id',item.$id)
                      .range('$pos',item.$pos)
                      .get(function (loaded)
                   {
                      _.extend(item,loaded);
                      done();
                   },{ attrs: query.projection.include })
                   .error(function (err)
                   {
                       if (err.code=='notfound')
                       {
                         items.splice(key,1);
                         done();
                       }
                       else
                         done(err);
                   });
              };

              async.forEach(_.range(items.length),
              _get,
              function (err)
              { 
                  if (err)
                    p.trigger.error(err);
                  else
                    p.trigger.results(items);
              });
           })
           .error(p.trigger.error);

       return p;

    };

    return finder;
};
