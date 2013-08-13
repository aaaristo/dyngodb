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
           hash= index.makeCondHash(query.cond),
           tab= dyn.table(query.index.name)
                   .hash('$hash',hash),
           sort;

       console.log('Query INDEX ('+index.name+') HASH',hash);

       if (query.orderby)
         Object.keys(query.orderby).every(function (field)
         {
             sort= { field: field, asc: modifiers.orderby[field]>0 };
             return false;
         });

       console.log('SORT',sort);

       if (sort)
         tab.index(sort.field);

       tab.query(p.trigger.results,
       { attrs: ['$id','$pos'], desc: sort&&!sort.asc, limit: query.limit })
       .error(p.trigger.error);

       return p;
    };

    return finder;
};
