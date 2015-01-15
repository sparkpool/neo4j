package org.neo4j.index.lucene;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;

import static java.util.concurrent.TimeUnit.DAYS;

@Ignore
public class TooManyDocumentsInIndexTest
{
    private static final int BATCH_SIZE = 10_000;

    private TargetDirectory testDir;

    private GraphDatabaseService db;
    private File graphDbDir;
    private Label label;
    private String propertyKey;

    @Before
    public void setUp() throws Exception
    {
        testDir = TargetDirectory.forTest( TooManyDocumentsInIndexTest.class );
        graphDbDir = testDir.cleanDirectory( "graph.db" );
        db = newDb();
        label = DynamicLabel.label( "Foo" );
        propertyKey = "Bar";

        createIndex( label, propertyKey );
    }

    @After
    public void tearDown() throws Exception
    {
        db.shutdown();
        testDir.cleanup();
    }

    @Test
    public void shouldThrowExceptionWhenIndexingLimitReached()
    {
        int shift = 3000;

        long t0 = System.currentTimeMillis();
        createNodes( Integer.MAX_VALUE - shift );
        System.out.println( "Write of (MAX_VALUE - " + shift + ") took: " + (System.currentTimeMillis() - t0) + "ms" );

        createNodes( shift );
    }

    @Test
    public void basicPerformanceTest()
    {
        long nodesToCreate = 10_000_000;

        long t0 = System.currentTimeMillis();

        createNodes( nodesToCreate );

        System.out.println( "Write of (" + nodesToCreate + ") took: " + (System.currentTimeMillis() - t0) + "ms" );
    }

    private void createNodes( long count )
    {
        long created = 0;
        while ( created < count - BATCH_SIZE )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int j = 0; j < BATCH_SIZE; j++ )
                {
                    createNode();
                }
                tx.success();

                created += BATCH_SIZE;

                if ( created % 100_000 == 0 )
                {
                    System.out.println( "created == " + created + ", count == " + count );
                }
            }

            if ( created % 10_000_000 == 0 )
            {
                System.out.println( "Restarting DB..." );
                restartDb();
            }
        }

        long left = count + BATCH_SIZE - created;
        try ( Transaction tx = db.beginTx() )
        {
            while ( left-- > 0 )
            {
                createNode();
            }
            tx.success();

            System.out.println( "Last batch: created == " + created + ", count == " + count );
        }
    }

    private void createNode()
    {
        Node node = db.createNode( label );
        node.setProperty( propertyKey, String.valueOf( ThreadLocalRandom.current().nextLong() ) );
    }

    private void createIndex( Label label, String propertyKey )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( propertyKey ).create();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 42, DAYS );
            tx.success();
        }
    }

    private void restartDb()
    {
        db.shutdown();
        System.gc();
        sleep();
        System.gc();
        sleep();
        db = newDb();
    }

    private GraphDatabaseService newDb()
    {
        return new GraphDatabaseFactory().newEmbeddedDatabase( graphDbDir.getAbsolutePath() );
    }

    private static void sleep()
    {
        try
        {
            Thread.sleep( 1_000 );
        }
        catch ( InterruptedException ignore )
        {
        }
    }
}

/*

Perf test:
    Write of (10000000) took: 213449ms
    Write of (10000000) took: 216889ms
    Write of (10000000) took: 213546ms

 */
