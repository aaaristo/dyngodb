var colors= require('colors'),
    _= require('underscore'),
    ret= require('ret');

//@FIXME: use divide&conquer (limit actually implemented in refiner)

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

       var p= dyn.promise(['results','count']);

       dyn.table(query.table.name)
              .scan(query.count ? p.trigger.count : p.trigger.results,
              { filter: query.filter, attrs: query.projection.include, count: query.count })
              .error(p.trigger.error);

       return p;
    };

    var buildFilter= function (query)
    {
       var cond= query.cond,
           filter= query.filter= {},
           canFind= true;

       Object.keys(cond).every(function (field)
       {
               var val= cond[field], type= typeof val,
                   _field= function (field,val,op)
                   {
                      filter[field]= { values: val===undefined ? [] : (Array.isArray(val) ? val : [ val ]),
                                           op: op };
                   },
                   _regexp= function (field,val)
                   {
                      var tks= ret(val.source),
                          _chars= function (start)
                          {
                              return _.collect(tks.stack.slice(start),
                                       function (tk) { return String.fromCharCode(tk.value); })
                                      .join(''); 
                          };

                      if (tks.stack
                          &&tks.stack[0]
                          &&tks.stack[0].type==ret.types.POSITION
                          &&tks.stack[0].value=='^'
                          &&!_.filter(tks.stack.slice(1),function (tk) { return tk.type!=ret.types.CHAR; }).length)
                      {
                        _field(field,_chars(1),'BEGINS_WITH');
                        return true;
                      }
                      else
                      if (tks.stack
                          &&!_.filter(tks.stack,function (tk) { return tk.type!=ret.types.CHAR; }).length)
                      {
                        _field(field,_chars(0),'CONTAINS');
                        return true;
                      }
                      else
                        return false;
                   };

               if (type=='object')
               {
                   if (Array.isArray(val))
                        _field(field,val,'IN');
                   else
                   {
                        if (val instanceof RegExp)
                          return canFind=_regexp(field,val);
                        else
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
                          return canFind= false;
                   }
               }
               else
                 _field(field,val,'EQ');

               return true;
       });

       return canFind;
    }

    return finder;
};
