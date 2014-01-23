var _= require('underscore'),
    JSOG= require('./JSOG'),
    async= require('async');


exports.clone= function (obj,cb)
{
    process.nextTick(function ()
    {
        cb(JSOG.parse(JSOG.stringify(obj)));
    });
};

exports.traverse= function (o, fn)
{
     var visited= [],
         _traverse= function (o,fn)
         {
             visited.push(o);
             Object.keys(o).forEach(function (i)
             {
                 var val= o[i];
                 fn.apply(null,[i,val,o]);

                 if (typeof (val)=='object'&&!_.contains(visited,val))
                   _traverse(val,fn);
             });
         };

      _traverse(o,fn);
};
