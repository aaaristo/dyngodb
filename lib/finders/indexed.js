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
              if (ind.usable(query.cond))
              {
                query.index= ind;
                return false;                        
              }
         });

         return !!query.index;
    };

    finder.find= function (query)
    {
       var p= dyn.promise(['results','count']),
           _get= function (item,done)
           {
               dyn.table(query.table.name)
                  .hash('$id',item.$id)
                  .range('$pos',item.$pos)
                  .get(function (loaded)
               {
                  _.extend(item,loaded);
                  done();
               },{ attrs: query.projection.include }).error(done);
           };

       console.log(('Query INDEX ('+query.index.name+')').yellow);

       var cursor= query.index.find(query)

       if (query.count)
           cursor.chain(p);
       else
           cursor.results(function (items)
           {
              async.forEach(items,_get,
              function (err)
              { 
                  if (err)
                    p.trigger.error(err);
                  else
                    p.trigger.results(items);
              });
           }).error(p.trigger.error);

       return p;

    };

    return finder;
};
