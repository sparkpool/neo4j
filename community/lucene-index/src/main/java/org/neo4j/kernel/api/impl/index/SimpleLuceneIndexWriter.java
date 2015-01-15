package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;

import java.io.IOException;

class SimpleLuceneIndexWriter extends LuceneIndexWriter
{
    SimpleLuceneIndexWriter( Directory directory, IndexWriterConfig config ) throws IOException
    {
        super( directory, config );
    }

    @Override
    void addDocument( Document document ) throws IOException
    {
        writer.addDocument( document );
    }

    @Override
    void updateDocument( Term term, Document document ) throws IOException
    {
        writer.updateDocument( term, document );
    }

    @Override
    void deleteDocuments( Term term ) throws IOException
    {
        writer.deleteDocuments( term );
    }
}
