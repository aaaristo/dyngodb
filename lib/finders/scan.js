var colors= require('colors');

module.exports= function (dyn)
{
    var finder= {};

    finder.canFind= function (query)
    {
       return buildFilter(query);
    };

    finder.find= function (query)
    {
       console.log(('SCAN on '+query.table.name+' for '+JSON.stringify(query.cond,null,2)).red);

       var p= dyn.promise('results');

       dyn.table(query.table.name)
              .scan(p.trigger.results,
              { filter: query.filter, attrs: query.projection.include, limit: query.limit })
              .error(p.trigger.error);

       return p;
    };

    var buildFilter= function (query)
    {
       var cond= query.cond,
           filter= query.filter= {},
           canFind= true;

       Object.keys(cond).forEach(function (field)
       {
               var val= cond[field], type= typeof val,
                   _field= function (field,val,op)
                   {
                      filter[field]= { values: val===undefined ? [] : (Array.isArray(val) ? val : [ val ]),
                                           op: op };
                   };

               if (type=='object')
               {
                   if (Array.isArray(val))
                        _field(field,val,'IN');
                   else
                   {
                        if (val.$ne!==undefined)
                          _field(field,val.$ne,'NE');
                        else
                        if (val.$gt!==undefined)
                          _field(field,val.$gt,'GT');
                        else
                        if (val.$lt!==undefined)
                          _field(field,val.$lt,'LT');
                        else
                        if (val.$gte!==undefined)
                          _field(field,val.$gte,'GE');
                        else
                        if (val.$lte!==undefined)
                          _field(field,val.$lte,'LE');
                        else
                        if (val.$in!==undefined)
                          _field(field,val.$in,'IN');
                        else
                        if (val.$exists!==undefined)
                          _field(field,undefined, val.$exists ? 'NOT_NULL' : 'NULL');
                        else
                        { canFind= false; p.trigger.error(new Error('Unknown conditional operator: '+JSON.stringify(val))); }
                   }
               }
               else
                 _field(field,val,'EQ');

       });

       return canFind;
    }

    return finder;
};
