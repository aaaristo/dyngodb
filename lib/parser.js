var _= require('underscore');

const _soa = function (o, s, v)
      {
            s = s.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
            s = s.replace(/^\./, '');           // strip a leading dot
            var a = s.split('.'),
                prop= a.pop();

            while (a.length) 
            {
                var n = a.shift();
                o = o[n] || (o[n]={});
            }

            o[prop]= v;
      };

module.exports= function (dyn)
{
    var parser= {};

    parser.parse= function (table,modifiers,cond,projection)
    {
        var p= dyn.promise('parsed'),
            query= { table: table, cond: cond || {}, projection: { root: {} } },
            _projection= function (root)
            {
              var proj= { include: ['$id','$ref'], exclude: []};

              _.keys(root).forEach(function (attr)
              {
                  if (root[attr].$include)
                  {
                    proj.include.push(attr);
                    proj.include.push('$$'+attr);
                    proj.include.push('$$$'+attr);
                  }
                  else
                  if (root[attr].$exclude)
                  {
                    proj.exclude.push(attr);
                    proj.exclude.push('$$'+attr);
                    proj.exclude.push('$$$'+attr);
                  }
              });

              if (proj.include.length==2)
                proj.include= undefined;

              return proj;
            };

        table.name= table._dynamo.TableName;
        table.indexes= table.indexes || [];

        query.project= _projection;

        query.toprojection= function (root)
        {
              var proj= {};

              _.keys(root || {}).forEach(function (attr)
              {
                  if (root[attr].$include)
                    proj[attr]= 1;
                  else
                  if (root[attr].$exclude)
                    proj[attr]= -1;
              });

              return proj;
        };
        
        process.nextTick(function ()
        {

            _.extend(query,modifiers);

            if (projection)
            {
                _.keys(projection).every(function (attr)
                { 
                       if (projection[attr]==1)
                         _soa(query.projection.root,attr,{ $include: true }); 
                       else
                       if (projection[attr]==-1)
                         _soa(query.projection.root,attr,{ $exclude: true }); 
                       else
                       {
                         p.trigger.error(new Error('unknown projection value '+JSON.stringify(projection[attr])));
                         return false;
                       }

                       return true;
                });

                _.extend(query.projection,query.project(query.projection.root));
            }

            p.trigger.parsed(query); 

        });

        return p;
    };

    return parser;
};
