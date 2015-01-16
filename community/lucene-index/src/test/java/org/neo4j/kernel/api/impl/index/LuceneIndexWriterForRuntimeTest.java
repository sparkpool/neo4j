/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.test.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LuceneIndexWriterForRuntimeTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void shouldReserveWhenMaxDocLimitIsNotReached() throws IOException
    {
        // Given
        int maxDocLimit = 42;
        int toReserve = maxDocLimit - 20;

        LuceneIndexWriterForRuntime indexWriter = newLuceneIndexWriterForRuntime( maxDocLimit );

        // When
        indexWriter.reserveDocumentAdditions( toReserve );

        // Then
        assertEquals( 0, indexWriter.createSearcherManager().acquire().getIndexReader().maxDoc() );
    }

    @Test
    public void shouldWorkIfSumOfMaxDocAndReservedIsLessThanLimit() throws IOException
    {
        // Given
        int maxDocLimit = 100;
        int toAdd = maxDocLimit / 2;
        int toReserve = maxDocLimit - toAdd - 7;

        LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        LuceneIndexWriterForRuntime indexWriter = newLuceneIndexWriterForRuntime( maxDocLimit );

        // When
        indexWriter.reserveDocumentAdditions( toAdd );
        for ( int i = 0; i < toAdd; i++ )
        {
            indexWriter.addDocument( documentStructure.newDocument( i ) );
        }

        indexWriter.reserveDocumentAdditions( toReserve );

        // Then
        assertEquals( toAdd, indexWriter.createSearcherManager().acquire().maxDoc() );
    }

    @Test( expected = LuceneIndexCapacityExceededException.class )
    public void shouldThrowIfMoreThanLimitDocsAreReserved() throws IOException
    {
        // Given
        int maxDocLimit = 42;
        int toReserve = maxDocLimit + 42;

        LuceneIndexWriterForRuntime indexWriter = newLuceneIndexWriterForRuntime( maxDocLimit );

        // When
        indexWriter.reserveDocumentAdditions( toReserve );

        // Then
        // exception is thrown
    }

    @Test( expected = LuceneIndexCapacityExceededException.class )
    public void shouldThrowWhenSumOfMaxDocAndReservedIsGreaterThanLimit() throws IOException
    {
        // Given
        int maxDocLimit = 100;
        int toAdd = maxDocLimit / 2;
        int toReserve = maxDocLimit - toAdd + 2;

        LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        LuceneIndexWriterForRuntime indexWriter = newLuceneIndexWriterForRuntime( maxDocLimit );

        // When
        indexWriter.reserveDocumentAdditions( toAdd );
        for ( int i = 0; i < toAdd; i++ )
        {
            indexWriter.addDocument( documentStructure.newDocument( i ) );
        }

        indexWriter.reserveDocumentAdditions( toReserve );

        // Then
        // exception is thrown
    }

    private LuceneIndexWriterForRuntime newLuceneIndexWriterForRuntime( long maxDocLimit ) throws IOException
    {
        File luceneDir = new File( "lucene" );
        fs.get().mkdir( luceneDir );
        Directory directory = new DirectoryFactory.InMemoryDirectoryFactory().open( luceneDir );
        IndexWriterConfig config = new IndexWriterConfig( Version.LUCENE_36, null );
        LuceneIndexWriterForRuntime indexWriter = new LuceneIndexWriterForRuntime( directory, config );
        LuceneIndexWriterForRuntime indexWriterSpy = spy( indexWriter );
        when( indexWriterSpy.maxDocLimit() ).thenReturn( maxDocLimit );
        return indexWriterSpy;
    }
}
