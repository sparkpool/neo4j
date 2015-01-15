package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

abstract class LuceneIndexWriter implements Closeable
{
    private static final long MAX_DOC_LIMIT = Integer.MAX_VALUE - 2048;

    protected final IndexWriter writer;

    LuceneIndexWriter( Directory dir, IndexWriterConfig conf ) throws IOException
    {
        this.writer = new IndexWriter( dir, conf );
    }

    abstract void addDocument( Document document ) throws IOException;

    abstract void updateDocument( Term term, Document document ) throws IOException;

    abstract void deleteDocuments( Term term ) throws IOException;

    IndexDeletionPolicy getIndexDeletionPolicy()
    {
        return writer.getConfig().getIndexDeletionPolicy();
    }

    void commit() throws IOException
    {
        writer.commit();
    }

    void commit( Map<String,String> commitUserData ) throws IOException
    {
        writer.commit( commitUserData );
    }

    @Override
    public void close() throws IOException
    {
        close( true );
    }

    void close( boolean waitForMerges ) throws IOException
    {
        writer.close( waitForMerges );
    }

    SearcherManager createSearcherManager() throws IOException
    {
        return new SearcherManager( writer, true, new SearcherFactory() );
    }

    void reserveDocumentAdditions( int additionsCount ) throws IOException
    {
        throw new UnsupportedOperationException( "Should not be called" );
    }

    long maxDocLimit()
    {
        return MAX_DOC_LIMIT;
    }
}
