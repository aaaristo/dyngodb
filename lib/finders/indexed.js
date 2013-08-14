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
       var p= dyn.promise('results','notfound'),
           index= query.index,
           hash= index.makeCondHash(query.cond),
           tab= dyn.table(query.index.name)
                   .hash('$hash',hash),
           sort,
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


       if (query.orderby)
         Object.keys(query.orderby).every(function (field)
         {
             sort= { field: field, asc: query.orderby[field]>0 };
             return false;
         });

       if (sort)
         tab.index(sort.field);

       console.log(('Query INDEX ('+index.name+') HASH '+hash+' SORT '+sort).yellow);

       tab.query(function (items)
       {
          async.forEach(items,_get,
          function (err)
          { 
              if (err)
                p.trigger.error(err);
              else
                p.trigger.results(items);
          });
       },
       { attrs: ['$id','$pos'], desc: sort&&!sort.asc, limit: query.limit })
       .error(p.trigger.error);

       return p;
    };

    return finder;
};
