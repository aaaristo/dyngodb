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
                return true;                        
              }
         });

         return !!query.index;
    };

    finder.find= function (query)
    {
       console.log(('Query INDEX ('+query.index.name+')').yellow);

       var p= dyn.promise(['results','count','end']),
           cursor= query.index.find(query);

       if (query.count)
           cursor.chain(p);
       else
           cursor.results(function (items)
           {
              items.forEach(function (item, idx) { items[idx]= { $ref: item }; }); 
              p.trigger.results(items);
           })
           .error(p.trigger.error)
           .end(p.trigger.end);

       return p;

    };

    return finder;
};
