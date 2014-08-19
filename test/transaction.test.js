var chai = require('chai'),
    spies = require('chai-spies'),
    AWS = require('aws-sdk'),
    _= require('underscore'),
    dyngo=  require('../index.js');

chai.use(spies);

var should= chai.should(),
    assert= chai.assert;

const noerr= function (done)
      {
         return function (err)
         {
             should.not.exist(err);
             done();
         };
      },
      accept= function (code,done)
      {
         return function (err)
         {
           if (err.code==code)
             done(); 
           else
             done(err);
         };
      };

describe('transactions',function ()
{
       var db;

       before(function (done)
       {
            dyngo({ dynamo: { endpoint: new AWS.Endpoint('http://localhost:8000') }, hints: false },
            function (err,_db)
            {
               db= _db; 

               db.ensureTransactionTable().success(done).error(done);
            });
       });

       beforeEach(function (done)
       {
            db.test.remove().success(done)
                            .error(done); 
       });

       describe('insert',function ()
       {

           it('If transaction A inserts a new object transaction B cannot see it while not committed (transient)',
           function (done)
           {
              db.transaction().transaction(function (A)
              {
                 db.transaction().transaction(function (B)
                 {
                     A.test.save({ _id: 'transient' }).success(function ()
                     {
                        B.test.findOne({ _id: 'transient' })
                              .result(should.not.exist)
                              .error(accept('notfound',done));
                     }).error(done);
                 }).error(done);
              }).error(done);
           });

           it('If transaction A inserts a new object, while there was a transient object already present, transaction B cannot see it while not committed (transient)',
           function (done)
           {
              db.transaction().transaction(function (A)
              {
                 db.transaction().transaction(function (B)
                 {
                     A.test.save({ _id: 'transient2' }).success(function ()
                     {
                         A.test.save({ _id: 'transient2' }).success(function ()
                         {
                            B.test.findOne({ _id: 'transient2' })
                                  .result(should.not.exist)
                                  .error(accept('notfound',done));
                         }).error(done);
                     }).error(done);
                 }).error(done);
              }).error(done);
           });

           it('If transaction A inserts a new object, and commit, transaction B can see the new object',
           function (done)
           {
              db.transaction().transaction(function (A)
              {
                 db.transaction().transaction(function (B)
                 {
                     A.test.save({ _id: 'transient3' }).success(function ()
                     {
                         A.commit().committed(function ()
                         {
                            B.test.findOne({ _id: 'transient3' })
                                  .result(function (r)
                                  {
                                      should.exist(r);
                                      r._id.should.equal('transient3');
                                      done();
                                  })
                                  .error(done);
                         }).error(done);
                     }).error(done);
                 }).error(done);
              }).error(done);
           });

       });

       describe('delete',function ()
       {

           it('If transaction A deletes an object and does not commit, transaction B can see the object, but transaction A can\'t',
           function (done)
           {
              db.test.save({ _id: 'delete1' }).success(function ()
              {
                  db.transaction().transaction(function (A)
                  {
                     db.transaction().transaction(function (B)
                     {
                         A.test.remove({ _id: 'delete1' }).success(function ()
                         {
                             B.test.findOne({ _id: 'delete1' })
                                   .result(function (r)
                                   {
                                      should.exist(r);
                                      r._id.should.equal('delete1');

                                      A.test.findOne({ _id: 'delete1' })
                                            .result(should.not.exist)
                                            .error(accept('notfound',done));
                                   })
                                   .error(done);

                         }).error(done);
                     }).error(done);
                  }).error(done);
              }).error(done);
           });

           it('If transaction A deletes an object and commit, transaction B cannot see the object',
           function (done)
           {
              db.test.save({ _id: 'delete2' }).success(function ()
              {
                  db.transaction().transaction(function (A)
                  {
                     db.transaction().transaction(function (B)
                     {
                         A.test.remove({ _id: 'delete2' }).success(function ()
                         {
                             A.commit().committed(function ()
                             {
                                 B.test.findOne({ _id: 'delete2' })
                                       .result(should.not.exist)
                                       .error(accept('notfound',done));
                             }).error(done);
                         }).error(done);
                     }).error(done);
                  }).error(done);
              }).error(done);
           });

       });

       describe('update',function ()
       {

           it('If transaction A updates an object and does not commit, transaction B still see the old version of the object, when A commits, B sees the new version',
           function (done)
           {
              var obj= { _id: 'update1', n: 0 };

              db.test.save(obj).success(function ()
              {
                  db.transaction().transaction(function (A)
                  {
                     db.transaction().transaction(function (B)
                     {
                         obj.name= 'Update2';
                         obj.n++;

                         A.test.save(obj).success(function ()
                         {
                             B.test.findOne({ _id: 'update1' })
                                   .result(function (copy)
                                   {
                                       should.not.exist(copy.name);
                                       copy.n.should.equal(0);

                                       A.test.findOne({ _id: 'update1' })
                                             .result(function (copy)
                                             {
                                                   copy.name.should.equal('Update2');
                                                   copy.n.should.equal(1);

                                                   A.commit().committed(function ()
                                                   {
                                                      B.test.findOne({ _id: 'update1' })
                                                            .result(function (copy)
                                                            {
                                                               copy.name.should.equal('Update2');
                                                               copy.n.should.equal(1);

                                                               done();
                                                            })
                                                            .error(done);
                                                   })
                                                   .error(done);
                                             })
                                             .error(done);
                                   })
                                   .error(done);
                         }).error(done);
                     }).error(done);
                  }).error(done);
              }).error(done);
           });

           it('If transaction A incs an object and does not commit, transaction B still see the old version of the object, when A commits, B sees the new version',
           function (done)
           {
              var obj= { _id: 'update2', n: 0 };

              db.test.save(obj).success(function ()
              {
                  db.transaction().transaction(function (A)
                  {
                     db.transaction().transaction(function (B)
                     {
                         A.test.update({},{ $inc: { n: 1 } }).success(function ()
                         {
                             B.test.findOne({ _id: 'update2' })
                                   .result(function (copy)
                                   {
                                       copy.n.should.equal(0);

                                       A.test.findOne({ _id: 'update2' })
                                             .result(function (copy)
                                             {
                                                   copy.n.should.equal(1);

                                                   A.commit().committed(function ()
                                                   {
                                                      B.test.findOne({ _id: 'update2' })
                                                            .result(function (copy)
                                                            {
                                                               copy.n.should.equal(1);

                                                               done();
                                                            })
                                                            .error(done);
                                                   })
                                                   .error(done);
                                             })
                                             .error(done);
                                   })
                                   .error(done);
                         }).error(done);
                     }).error(done);
                  }).error(done);
              }).error(done);
           });

           it('When some object is updated more than one time the original copy is rolled back',function (done)
           {
               db.test.save({ _id: 'update-orig', n: 1 })
                      .success(function ()
               {
                   db.transaction().transaction(function (tx)
                   {
                       tx.test.findOne({ _id: 'update-orig' })
                              .result(function (obj)
                       {
                           obj.n++;
                           tx.test.save(obj)
                                  .success(function ()
                           {
                               obj.n++;
                               tx.test.save(obj)
                                      .success(function ()
                               {
                                   tx.rollback().rolledback(function ()
                                   {
                                        db.test.findOne({ _id: 'update-orig' })
                                               .result(function (obj)
                                        {
                                            obj.n.should.equal(1); 
                                            done();
                                        })
                                        .error(done);      
                                   })
                                   .error(done);      
                               })
                               .error(done); 
                           })
                           .error(done); 
                       })
                       .error(done);
                   })
                  .error(done);
               })
               .error(done);
           });
       });

       describe('query',function ()
       {
           it('If transaction A inserts an item without committing, transaction B should not see the inserted item until commited',
           function (done)
           {
               var items= _.collect(_.range(10),function (n) { return { _id: 'item'+n, n: n+1 } });

               db.test.save(items).success(function ()
               {
                  db.transaction().transaction(function (A)
                  {
                     db.transaction().transaction(function (B)
                     {
                         A.test.save({ _id: 'itemS', n: 0 }).success(function ()
                         {
                             B.test.find().sort({ n: 1 }).limit(3)
                                   .results(function (objs)
                                   {
                                      objs[0].n.should.equal(1);
                                      objs.length.should.equal(3);
                                   })
                                   .error(done)
                                   .end(function ()
                                   {
                                     A.test.find().sort({ n: 1 }).limit(3)
                                           .results(function (objs)
                                           {
                                              objs[0].n.should.equal(0);
                                              objs.length.should.equal(3);
                                           })
                                           .error(done)
                                           .end(function ()
                                           {
                                               A.commit().committed(function ()
                                               {
                                                     B.test.find().sort({ n: 1 }).limit(3)
                                                           .results(function (objs)
                                                           {
                                                              objs[0].n.should.equal(0);
                                                              objs.length.should.equal(3);
                                                           })
                                                           .error(done)
                                                           .end(done);
                                               });
                                           });
                                   });
                         }).error(done);
                     });   
                  });   
               });  
           });
       });

       describe('concurrency',function ()
       {
           it('rollsback competing transaction',
           function (done)
           {
               db.test.save({ _id: 'hot', name: 'Hot' }).success(function ()
               {
                   db.transaction().transaction(function (A)
                   {
                       db.transaction().transaction(function (B)
                       {
                          A.test.findOne({ _id: 'hot' })
                                .result(function (hotA)
                                {
                                   hotA.name= 'HotA';

                                   B.test.findOne({ _id: 'hot' })
                                         .result(function (hotB)
                                         {
                                            hotB.name= 'HotB';

                                            A.test.save(hotA).success(function ()
                                            {
                                                B.test.save(hotB).success(function ()
                                                {
                                                    var committedA= chai.spy();

                                                    A.commit()
                                                     .committed(committedA)
                                                     .error(function (err)
                                                    {
                                                        if (err.code=='rolledback')
                                                          B.commit().committed(function ()
                                                          {
                                                              db.test.findOne({ _id: 'hot' })
                                                                     .result(function (obj)
                                                                      {
                                                                         committedA.should.not.have.been.called();
                                                                         obj.name.should.equal('HotB');
                                                                         done();
                                                                      })
                                                                     .error(done);
                                                          })
                                                          .error(done);
                                                        else
                                                          done(err);
                                                    });
                                                })
                                                .error(done);
                                            })
                                            .error(done);
                                         })
                                         .error(done);
                                })
                                .error(done);

                       }).error(done);
                   }).error(done);
               }).error(done);
           });
       });
});
