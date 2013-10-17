var _finders= [require('./finders/simple'),
               require('./finders/indexed'),
               require('./finders/scan')];

module.exports= function (dyn)
{
    var finder= {};

    finder.find= function (query)
    {
        var canFind= false,
            p= dyn.promise(['results','count','end'],'notfound','consumed');

        process.nextTick(function ()
        {

            _finders.every(function (f)
            {
               var _finder= f(dyn);

               if (_finder.canFind(query))
               {
                  canFind= true;
                  _finder.find(query)
                         .chain(p);
                  return false;
               }
               
               return true;
            }); 

            if (!canFind)
              p.trigger.error(new Error('Cannot handle that query yet: '+JSON.stringify(query.cond)));

        });

        return p;
    };

    return finder;
};
