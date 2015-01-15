/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.kernel.api.impl.index.IndexWriterFactories.standard;

public class LuceneIndexAccessorTest
{

    @Test
    public void indexReaderShouldHonorRepeatableReads() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // WHEN
        updateAndCommit( asList( remove( nodeId, value ) ) );

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }

    @Test
    public void multipleIndexReadersFromDifferentPointsInTimeCanSeeDifferentResults() throws Exception
    {
        // WHEN
        updateAndCommit( asList( add( nodeId, value ) ) );
        IndexReader firstReader = accessor.newReader();
        updateAndCommit( asList( add( nodeId2, value ) ) );
        IndexReader secondReader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( firstReader.lookup( value ) ) );
        assertEquals( asSet( nodeId, nodeId2 ), asUniqueSet( secondReader.lookup( value ) ) );
        firstReader.close();
        secondReader.close();
    }
    
    @Test
    public void canAddNewData() throws Exception
    {
        // WHEN
        updateAndCommit( asList(
                add( nodeId, value ),
                add( nodeId2, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }
    
    @Test
    public void canChangeExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList( add( nodeId, value ) ) );

        // WHEN
        updateAndCommit( asList( change( nodeId, value, value2 ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId ), asUniqueSet( reader.lookup( value2 ) ) );
        assertEquals( emptySetOf( Long.class ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }
    
    @Test
    public void canRemoveExistingData() throws Exception
    {
        // GIVEN
        updateAndCommit( asList(
                add( nodeId, value ),
                add( nodeId2, value ) ) );

        // WHEN
        updateAndCommit( asList( remove( nodeId, value ) ) );
        IndexReader reader = accessor.newReader();

        // THEN
        assertEquals( asSet( nodeId2 ), asUniqueSet( reader.lookup( value ) ) );
        reader.close();
    }

    @Test
    public void updaterShouldReserveDocuments() throws IOException
    {
        // Given
        LuceneIndexWriter indexWriter = mock( LuceneIndexWriter.class );
        LuceneIndexWriterFactory indexWriterFactory = mock( LuceneIndexWriterFactory.class );
        when( indexWriterFactory.create( any( Directory.class ) ) ).thenReturn( indexWriter );

        NonUniqueLuceneIndexAccessor indexAccessor =
                new NonUniqueLuceneIndexAccessor( documentLogic, indexWriterFactory, writerLogic, dirFactory, dir );

        IndexUpdater updater = indexAccessor.newUpdater( IndexUpdateMode.ONLINE );

        // When
        updater.validate( asList(
                NodePropertyUpdate.add( 1, 1, null, null ),
                NodePropertyUpdate.add( 2, 2, null, null ),
                NodePropertyUpdate.add( 3, 3, null, null ) ) );

        updater.validate( asList(
                NodePropertyUpdate.change( 0, 0, null, null, null, null ),
                NodePropertyUpdate.add( 1, 1, null, null ),
                NodePropertyUpdate.add( 2, 2, null, null ),
                NodePropertyUpdate.remove( 3, 3, null, null ) ) );

        updater.validate( asList(
                NodePropertyUpdate.change( 0, 0, null, null, null, null ),
                NodePropertyUpdate.change( 1, 1, null, null, null, null ),
                NodePropertyUpdate.remove( 2, 2, null, null ),
                NodePropertyUpdate.remove( 3, 3, null, null ) ) );

        // Then
        InOrder order = inOrder( indexWriter );
        order.verify( indexWriter ).createSearcherManager();
        order.verify( indexWriter ).reserveDocumentAdditions( 3 );
        order.verify( indexWriter ).reserveDocumentAdditions( 2 );
        verifyNoMoreInteractions( indexWriter );
    }

    private final long nodeId = 1, nodeId2 = 2;
    private final Object value = "value", value2 = 40;
    private final LuceneDocumentStructure documentLogic = new LuceneDocumentStructure();
    private final IndexWriterStatus writerLogic = new IndexWriterStatus();
    private final File dir = new File( "dir" );
    private LuceneIndexAccessor accessor;
    private DirectoryFactory.InMemoryDirectoryFactory dirFactory;
    
    @Before
    public void before() throws Exception
    {
        dirFactory = new DirectoryFactory.InMemoryDirectoryFactory();
        accessor = new NonUniqueLuceneIndexAccessor( documentLogic, standard(), writerLogic, dirFactory, dir );
    }

    @After
    public void after()
    {
        dirFactory.close();
    }

    private NodePropertyUpdate add( long nodeId, Object value )
    {
        return NodePropertyUpdate.add( nodeId, 0, value, new long[0] );
    }
    
    private NodePropertyUpdate remove( long nodeId, Object value )
    {
        return NodePropertyUpdate.remove( nodeId, 0, value, new long[0] );
    }

    private NodePropertyUpdate change( long nodeId, Object valueBefore, Object valueAfter )
    {
        return NodePropertyUpdate.change( nodeId, 0, valueBefore, new long[0], valueAfter, new long[0] );
    }

    private void updateAndCommit( List<NodePropertyUpdate> nodePropertyUpdates )
            throws IOException, IndexEntryConflictException
    {
        try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            for ( NodePropertyUpdate update : nodePropertyUpdates )
            {
                updater.process( update );
            }
        }
    }
}
