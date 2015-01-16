/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha.transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.neo4j.com.Response;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.util.JobScheduler.Group.masterTransactionPushing;

public class CommitPusher extends LifecycleAdapter
{
    private static class PullUpdateFuture
            extends FutureTask<Object>
    {
        private Slave slave;
        private long txId;

        public PullUpdateFuture( Slave slave, long txId )
        {
            super( new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    return null;
                }
            });
            this.slave = slave;
            this.txId = txId;
        }

        @Override
        public void done()
        {
            super.set( null );
            super.done();
        }

        @Override
        public void setException( Throwable t )
        {
            super.setException( t );
        }

        public Slave getSlave()
        {
            return slave;
        }

        private long getTxId()
        {
            return txId;
        }
    }

    private final Map<Integer, BlockingQueue<PullUpdateFuture>> pullUpdateQueues = new HashMap<>(  );
    private final JobScheduler scheduler;

    public CommitPusher( JobScheduler scheduler )
    {
        this.scheduler = scheduler;
    }

    public void queuePush( final XaDataSource dataSource, Slave slave, final long txId )
    {
        PullUpdateFuture pullRequest = new PullUpdateFuture(slave, txId);

        BlockingQueue<PullUpdateFuture> queue = pullUpdateQueues.get( slave.getServerId() );

        // Create a new queue if needed
        queue = queue == null ? createNewQueue( dataSource, slave ) : queue;

        // Add our request to the queue
        while( !queue.offer( pullRequest ) )
        {
            Thread.yield();
        }

        try
        {
            // Wait for request to finish
            pullRequest.get();
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted(); // Clear interrupt flag
            throw new RuntimeException( e );
        }
        catch ( ExecutionException e )
        {
            if (e.getCause() instanceof RuntimeException)
                throw ((RuntimeException)e.getCause());
            else
                throw new RuntimeException( e.getCause() );
        }
    }

    private synchronized BlockingQueue<PullUpdateFuture> createNewQueue( final XaDataSource dataSource, Slave slave )
    {
        BlockingQueue<PullUpdateFuture> queue = pullUpdateQueues.get( slave.getServerId() );
        if (queue == null)
        {
            // Create queue and worker
            queue = new ArrayBlockingQueue<>( 100 );
            pullUpdateQueues.put( slave.getServerId(), queue );

            final BlockingQueue<PullUpdateFuture> finalQueue = queue;
            scheduler.schedule( masterTransactionPushing, new Runnable()
            {
                List<PullUpdateFuture> currentPulls = new ArrayList<>();

                @Override
                public void run()
                {
                    try
                    {
                        while (true)
                        {
                            // Poll queue and call pullUpdate
                            currentPulls.clear();
                            currentPulls.add( finalQueue.take() );

                            finalQueue.drainTo( currentPulls );

                            try
                            {
                                PullUpdateFuture pullUpdateFuture = currentPulls.get( 0 );
                                Response<Void> response = pullUpdateFuture.getSlave().pullUpdates( dataSource.getName(), pullUpdateFuture.getTxId() );
                                response.close();

                                // Notify the futures
                                for ( PullUpdateFuture currentPull : currentPulls )
                                {
                                    currentPull.done();
                                }
                            }
                            catch ( Exception e )
                            {
                                // Notify the futures
                                for ( PullUpdateFuture currentPull : currentPulls )
                                {
                                    currentPull.setException( e );
                                }
                            }
                        }
                    }
                    catch ( InterruptedException e )
                    {
                        // Quit
                    }
                }
            } );
        }
        return queue;
    }

}
