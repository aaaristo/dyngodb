module.exports= function (dyn)
{
    var finder= {};

    finder.canFind= function (query)
    {
       return !!query.cond.$id&&!(query.cond.$id instanceof RegExp);
    };

    finder.find= function (query)
    {
       console.log(('PK on '+query.table.name+' for '+JSON.stringify(query.cond,null,2)).green);

       var p= dyn.promise('results','notfound');

       if (query.cond.$pos!==undefined)
         dyn.table(query.table.name)
            .hash('$id',query.cond.$id)
            .range('$pos',query.cond.$pos)
            .get(function (item)
            {
                 p.trigger.results([item]);
            },{ attrs: query.projection.include })
            .error(p.trigger.error);
       else
       if (query.limit==1)
         dyn.table(query.table.name)
            .hash('$id',query.cond.$id)
            .range('$pos',0)
            .query(p.trigger.results,{ attrs: query.projection.include })
            .error(p.trigger.error);
       else
         dyn.table(query.table.name)
            .hash('$id',query.cond.$id)
            .query(p.trigger.results,{ attrs: query.projection.include, limit: query.limit })
            .error(p.trigger.error);

       return p;
    };

    return finder;
};
