var _= require('underscore'),
    async= require('async');


exports.clone= function (obj,cb)
{
     var knownObjects= [],
         __clone= function (src,cb)
         {
            var dst= Array.isArray(src) ? [] : {};

            process.nextTick(function ()
            { 
                  async.forEach(Object.keys(src),
                  function (key,done)
                  {
                     var val= src[key];

                     if (typeof val=='object')
                       dst[key]= _clone(val,function () { process.nextTick(done); }) 
                     else
                     {
                       dst[key]= val;
                       done();
                     }
                  },
                  cb);
            });

            return dst;
         },
         _clone= function (obj,cb)
         {
            var known;

            if (!(known= _.findWhere(knownObjects,{ src: obj })))
               knownObjects.push(known={ src: obj, dst: __clone(obj,function ()
               {
                  cb(known.dst);
               }) });
            else
               cb(known.dst);

            return known.dst;
         };

     _clone(obj,cb);
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
