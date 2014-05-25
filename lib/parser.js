var _= require('underscore'),
    ret= require('ret');

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
      },
      _oa = function(o, s) 
      {
             s = s.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
             s = s.replace(/^\./, '');           // strip a leading dot
             var a = s.split('.');
             while (a.length) {
                 var n = a.shift();
                 if (n in o) {
                     o = o[n];
                 } else {
                     return;
                 }
             }
             return o;
      },
      _buildFilter= function (query)
      {
           var cond= query.cond,
               filter= query.$filter= {},
               canFind= true;

           if (cond.$text)
           {
             query.$text= cond.$text;
             cond= query.cond= _.omit(query.cond,'$text');
           }
            
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
                            var val= _chars(1);
                            if (val!='')
                              _field(field,val,'BEGINS_WITH');
                          }
                          else
                          if (tks.stack
                              &&!_.filter(tks.stack,function (tk) { return tk.type!=ret.types.CHAR; }).length)
                          {
                            var val= _chars(0);
                            if (val!='')
                              _field(field,val,'CONTAINS');
                          }
                          else
                              _field(field,val,'REGEXP');
                       };

                   if (type=='object')
                   {
                       if (Array.isArray(val))
                            return canFind= false;
                       else
                       {
                            if (val instanceof RegExp)
                              _regexp(field,val);
                            else
                            if (val.$gte!==undefined&&val.$lte!==undefined)
                              _field(field,[val.$gte,val.$lte],'BETWEEN');
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
                            if (val.$all!==undefined)
                              _field(field,val.$all,'ALL');
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
      };


module.exports= function (dyn,opts)
{
    var parser= {};

    parser.parse= function (table,modifiers,cond,projection,identity)
    {
        var p= dyn.promise('parsed'),
            query= { table: table, cond: cond || {}, projection: { root: {} }, opts: opts },
            _projection= function (root)
            {
              var proj= { include: ['_id','_pos','_ref'], exclude: []};

              _.keys(root).forEach(function (attr)
              {
                  if (root[attr].$include)
                  {
                    proj.include.push(attr);
                    proj.include.push('__'+attr);
                    proj.include.push('___'+attr);
                  }
                  else
                  if (root[attr].$exclude)
                  {
                    proj.exclude.push(attr);
                    proj.exclude.push('__'+attr);
                    proj.exclude.push('___'+attr);
                  }
              });

              proj.include= _.uniq(proj.include);
              proj.exclude= _.uniq(proj.exclude);

              if (proj.include.length==3)
                proj.include= undefined;

              return proj;
            };

        table.name= table._dynamo.TableName;
        table.indexes= table.indexes || [];

        query.project= _projection;
        query.soa= _soa;
        query.oa= _oa;

        if (modifiers.orderby)
        {
               var fields= [];

               Object.keys(modifiers.orderby).forEach(function (name)
               {
                   fields.push({ name: name, dir: modifiers.orderby[name] });
               });

               query.$orderby= fields;
        }

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

        if (projection&&_.keys(projection).length)
        {
          query.identity= {};

          query.identity.get= function (_id, _pos, done)
          {
             done();
          };

          query.identity.set= function (item)
          {
          };
        }
        else
        {
          if (identity&&identity.map)
              query.identity= identity;
          else
          {
              query.identity= {};
              query.identity.map= {};

              query.identity.get= function (_id, _pos, cb)
              {
                 var item= query.identity.map[_id+':'+_pos];

                 if (!item)
                 {
                   query.identity.map[_id+':'+_pos]= true;
                   cb();
                 }
                 else
                 if (item===true)
                   setTimeout(query.identity.get,100,_id,_pos,cb);
                 else
                   cb(item);
              }

              query.identity.set= function (_id, _pos, item)
              {
                 query.identity.map[_id+':'+_pos]= item;
              }
          }
        }
        
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

            query.$supported= _buildFilter(query);
            query.$returned= 0;
            query.$filtered= [];
            query.window= query.window || 50;

            query.filterComplete= function ()
            {
               return Object.keys(query.$filter).length==0;
            };

            query.sortComplete= function ()
            {
               return !(query.orderby&&!query.sorted);
            };

            query.limitComplete= function ()
            {
               return !(query.limit!==undefined&&!query.limited);
            };

            query.skipComplete= function ()
            {
               return !(query.skip!==undefined&&!query.skipped);
            };

            query.canLimit= function ()
            {
               return !query.limited 
                    &&query.sortComplete()
                    &&query.filterComplete();
            };

            query.canSkip= function ()
            {
               return !query.skipped 
                    &&query.sortComplete()
                    &&query.filterComplete();
            };

            query.canCount= function ()
            {
               return query.filterComplete()
                    &&query.limitComplete()
                    &&query.skipComplete();
            };

            query.finderProjection= function ()
            {
               if (query.projection.include)
               {
                   // if i cannot filter them i project them to the refiner for client-side filtering
                   var toproject= _.difference(_.keys(query.$filter),query.projection.include) || [];
                   return _.union(query.projection.include,toproject);
               }
               else
                 return undefined;
            };

            p.trigger.parsed(query); 

        });

        return p;
    };

    return parser;
};
