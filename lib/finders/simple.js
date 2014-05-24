var _= require('underscore');

module.exports= function (dyn)
{
    var finder= {};

    finder.canFind= function (query)
    {
       return !!query.cond._id&&!(query.cond._id instanceof RegExp);
    };

    finder.find= function (query)
    {
       if (query.opts.hints)
         console.log(('PK on '+query.table.name+' for '+JSON.stringify(query.cond,null,2)).green);

       if (query.cond._id)
         delete query.$filter._id;

       if (query.cond._pos!==undefined)
         delete query.$filter._pos;

       var p= dyn.promise(['results','count','end'],'notfound','consumed'),
           _triggerError= query.count ? function (err)
           {
             if (err.code=='notfound')
               p.trigger.count(0);
             else 
               p.trigger.error(err);
           } : p.trigger.error;

       if (query.cond._pos!==undefined)
         dyn.table(query.table.name)
            .hash('_id',query.cond._id)
            .range('_pos',query.cond._pos)
            .get(function (item)
            {
                 if (query.count)
                   p.trigger.count(1);
                 else
                 {
                   p.trigger.results([item]);
                   p.trigger.end();
                 }
            },{ attrs: query.finderProjection(),
           consistent: query.$consistent })
            .consumed(p.trigger.consumed)
            .error(_triggerError);
       else
       if (query.limit==1)
         dyn.table(query.table.name)
            .hash('_id',query.cond._id)
            .range('_pos',0)
            .query(query.count ? p.trigger.count : p.trigger.results,
             { attrs: query.finderProjection(), count: query.count, consistent: query.$consistent })
            .error(_triggerError)
            .consumed(p.trigger.consumed)
            .end(p.trigger.end);
       else
         dyn.table(query.table.name)
            .hash('_id',query.cond._id)
            .query(query.count ? p.trigger.count : p.trigger.results,
             { attrs: query.finderProjection(), limit: query.limit, count: query.count, consistent: query.$consistent })
            .error(_triggerError)
            .consumed(p.trigger.consumed)
            .end(p.trigger.end);

       return p;
    };

    return finder;
};
