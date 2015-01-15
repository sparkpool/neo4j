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

public class LuceneIndexWriterForPopulationTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void shouldSilentlyAddDocumentsWhenMaxDocIsLessThanLimit() throws IOException
    {
        // Given
        int maxDocLimit = 1_000;
        int toAdd = maxDocLimit - 10;

        LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        LuceneIndexWriterForPopulation indexWriter = newLuceneIndexWriterForPopulation( maxDocLimit );

        // When
        for ( int i = 0; i < toAdd; i++ )
        {
            indexWriter.addDocument( documentStructure.newDocument( i ) );
        }

        // Then
        assertEquals( toAdd, indexWriter.createSearcherManager().acquire().getIndexReader().numDocs() );
    }

    @Test( expected = LuceneIndexCapacityExceededException.class )
    public void shouldThrowExceptionWhenTooManyDocumentsAreAdded() throws IOException
    {
        // Given
        int maxDocLimit = 1_000;
        int toAdd = maxDocLimit + 10;

        LuceneDocumentStructure documentStructure = new LuceneDocumentStructure();
        LuceneIndexWriterForPopulation indexWriter = newLuceneIndexWriterForPopulation( maxDocLimit );

        // When
        for ( int i = 0; i < toAdd; i++ )
        {
            indexWriter.addDocument( documentStructure.newDocument( i ) );
        }

        // Then
        // exception is thrown
    }

    private LuceneIndexWriterForPopulation newLuceneIndexWriterForPopulation( long maxDocLimit ) throws IOException
    {
        File luceneDir = new File( "lucene" );
        fs.get().mkdir( luceneDir );
        Directory directory = new DirectoryFactory.InMemoryDirectoryFactory().open( luceneDir );
        IndexWriterConfig config = new IndexWriterConfig( Version.LUCENE_36, null );
        LuceneIndexWriterForPopulation indexWriter = new LuceneIndexWriterForPopulation( directory, config );
        LuceneIndexWriterForPopulation indexWriterSpy = spy( indexWriter );
        when( indexWriterSpy.maxDocLimit() ).thenReturn( maxDocLimit );
        return indexWriterSpy;
    }
}
