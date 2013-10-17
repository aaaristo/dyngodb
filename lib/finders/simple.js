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

       query.$filter= {};

       var p= dyn.promise(['results','count','end'],'notfound'),
           _triggerError= query.count ? function (err)
           {
             if (err.code=='notfound')
               p.trigger.count(0);
             else 
               p.trigger.error(err);
           } : p.trigger.error;

       if (query.cond.$pos!==undefined)
         dyn.table(query.table.name)
            .hash('$id',query.cond.$id)
            .range('$pos',query.cond.$pos)
            .get(function (item)
            {
                 if (query.count)
                   p.trigger.count(1);
                 else
                 {
                   p.trigger.results([item]);
                   p.trigger.end();
                 }
            },{ attrs: query.projection.include, consistent: query.$consistent })
            .error(_triggerError);
       else
       if (query.limit==1)
         dyn.table(query.table.name)
            .hash('$id',query.cond.$id)
            .range('$pos',0)
            .query(query.count ? p.trigger.count : p.trigger.results,
             { attrs: query.projection.include, count: query.count, consistent: query.$consistent })
            .error(_triggerError)
            .end(p.trigger.end);
       else
         dyn.table(query.table.name)
            .hash('$id',query.cond.$id)
            .query(query.count ? p.trigger.count : p.trigger.results,
             { attrs: query.projection.include, limit: query.limit, count: query.count, consistent: query.$consistent })
            .error(_triggerError)
            .end(p.trigger.end);

       return p;
    };

    return finder;
};
