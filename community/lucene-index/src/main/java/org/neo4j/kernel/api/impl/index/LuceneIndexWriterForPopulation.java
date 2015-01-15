package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

import java.io.IOException;

class LuceneIndexWriterForPopulation extends SimpleLuceneIndexWriter
{
    LuceneIndexWriterForPopulation( Directory directory, IndexWriterConfig config ) throws IOException
    {
        super( directory, config );
    }

    @Override
    void addDocument( Document document ) throws IOException
    {
        checkMaxDoc();
        super.addDocument( document );
    }

    private void checkMaxDoc() throws IOException
    {
        int currentMaxDoc = writer.maxDoc();
        if ( currentMaxDoc > maxDocLimit() )
        {
            throw new LuceneIndexCapacityExceededException(
                    "Lucene index contains too many documents. Current limitation is " + maxDocLimit() + " documents " +
                    "per index. Current value of maxDoc is " + currentMaxDoc + "." );
        }
    }
}
