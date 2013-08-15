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
       var p= dyn.promise('results'),
           _get= function (item,done)
           {
               dyn.table(query.table.name)
                  .hash('$id',item.$id)
                  .range('$pos',item.$pos)
                  .get(function (loaded)
               {
                  _.extend(item,loaded);
                  done();
               }).error(done);
           };

       console.log(('Query INDEX ('+query.index.name+')').yellow);

       query.index.find(query).results(function (items)
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
