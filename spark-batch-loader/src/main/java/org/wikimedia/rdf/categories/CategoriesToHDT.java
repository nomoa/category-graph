package org.wikimedia.rdf.categories;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.rdfhdt.hdt.dictionary.TempDictionary;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTVocabulary;
import org.rdfhdt.hdt.hdt.TempHDT;
import org.rdfhdt.hdt.hdt.impl.HDTImpl;
import org.rdfhdt.hdt.hdt.impl.ModeOfLoading;
import org.rdfhdt.hdt.hdt.impl.TempHDTImpl;
import org.rdfhdt.hdt.header.HeaderUtil;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf.RDFParserCallback;
import org.rdfhdt.hdt.rdf.RDFParserFactory;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.listener.ListenerUtil;

public final class CategoriesToHDT {
    private CategoriesToHDT() {}
    public static void main(String[] args) throws IOException, ParserException {
        // XXX: causes OOM on large datasets, might investigate why
        // might need to experiment with https://github.com/rdfhdt/hdt-mr
        List<URL> dumps = IOUtils.readLines(CategoriesToHDT.class.getResourceAsStream("/commons.lst"), StandardCharsets.UTF_8).stream().map(e -> {
            try {
                return new URL("https://dumps.wikimedia.your.org/other/categoriesrdf/20210918/" + e);
            } catch (MalformedURLException ex) {
                throw new IllegalArgumentException("meh");
            }
        }).collect(Collectors.toList());
        HDT hdt = parseCategories(dumps);
        hdt.saveToHDT("/tmp/all_categ.hdt", (v, s) -> {
        });
    }

    private static HDT parseCategories(List<URL> dumps) throws IOException, ParserException {
        HDTSpecification spec = new HDTSpecification();
        ProgressListener listener = (v, s) -> {
        };
        RDFParserCallback callback = RDFParserFactory.getParserCallback(RDFNotation.TURTLE);
        TempHDT modHDT = new TempHDTImpl(new HDTSpecification(), "uri:unused", ModeOfLoading.ONE_PASS);
        TempDictionary dictionary = modHDT.getDictionary();
        TempTriples triples = modHDT.getTriples();
        TripleAppender appender = new TripleAppender(dictionary, triples, listener);
        dictionary.startProcessing();
        for (URL dump : dumps) {
            try (InputStream is = new GZIPInputStream(dump.openStream())) {
                callback.doParse(is, "uri:unused", RDFNotation.TURTLE, appender);
            }
        }
        dictionary.endProcessing();

        modHDT.reorganizeDictionary(listener);
        modHDT.reorganizeTriples(listener);

        modHDT.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, appender.size);

        HDTImpl hdt = new HDTImpl(spec);
        hdt.loadFromModifiableHDT(modHDT, listener);
        hdt.populateHeaderStructure(modHDT.getBaseURI());

        // Add file size to Header
        try {
            long originalSize = HeaderUtil.getPropertyLong(modHDT.getHeader(), "_:statistics", HDTVocabulary.ORIGINAL_SIZE);
            hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, originalSize);
        } catch (NotFoundException e) {
        }

        modHDT.close();
        return hdt;
    }

    static class TripleAppender implements RDFParserCallback.RDFCallback {
        final TempDictionary dict;
        final TempTriples triples;
        final ProgressListener listener;
        long num;
        long size;

        TripleAppender(TempDictionary dict, TempTriples triples, ProgressListener listener) {
            this.dict = dict;
            this.triples = triples;
            this.listener = listener;
        }

        @Override
        public void processTriple(TripleString triple, long pos) {
            triples.insert(
                    dict.insert(triple.getSubject(), TripleComponentRole.SUBJECT),
                    dict.insert(triple.getPredicate(), TripleComponentRole.PREDICATE),
                    dict.insert(triple.getObject(), TripleComponentRole.OBJECT)
            );
            num++;
            size += triple.getSubject().length() + triple.getPredicate().length() + triple.getObject().length() + 4;  // Spaces and final dot
            ListenerUtil.notifyCond(listener, "Loaded " + num + " triples", num, 0, 100);
        }
    }
}
