package org.corpus_tools.pepperModules_SqueezerModule;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.corpus_tools.pepper.common.DOCUMENT_STATUS;
import org.corpus_tools.pepper.common.PepperConfiguration;
import org.corpus_tools.pepper.impl.PepperManipulatorImpl;
import org.corpus_tools.pepper.impl.PepperMapperImpl;
import org.corpus_tools.pepper.modules.PepperManipulator;
import org.corpus_tools.pepper.modules.PepperMapper;
import org.corpus_tools.pepper.modules.PepperModule;
import org.corpus_tools.pepper.modules.PepperModuleProperties;
import org.corpus_tools.pepper.modules.exceptions.PepperModuleDataException;
import org.corpus_tools.pepper.modules.exceptions.PepperModuleNotReadyException;
import org.corpus_tools.salt.SALT_TYPE;
import org.corpus_tools.salt.SaltFactory;
import org.corpus_tools.salt.common.*;
import org.corpus_tools.salt.core.GraphTraverseHandler;
import org.corpus_tools.salt.core.SAnnotation;
import org.corpus_tools.salt.core.SGraph.GRAPH_TRAVERSE_TYPE;
import org.corpus_tools.salt.core.SLayer;
import org.corpus_tools.salt.core.SNode;
import org.corpus_tools.salt.core.SRelation;
import org.corpus_tools.salt.graph.Identifier;
import org.eclipse.emf.common.util.URI;
import org.osgi.service.component.annotations.Component;

/**
 * This is a {@link PepperManipulator} which can move annotations, for 
 * example from edges to their attached source or target nodes.
 *
 * @author Luke Gessler
 */
@Component(name = "SqueezerModuleManipulatorComponent", factory = "PepperManipulatorComponentFactory")
public class SqueezerModuleManipulator extends PepperManipulatorImpl {

    public SqueezerModuleManipulator() {
        super();
        setName("Squeezer");
        setSupplierContact(URI.createURI(PepperConfiguration.EMAIL));
        setSupplierHomepage(URI.createURI(PepperConfiguration.HOMEPAGE));
        setDesc("This manipulator deletes nodes with no annotations.");
    }

    /**
     * @param Identifier
     *            {@link Identifier} of the {@link SCorpus} or {@link SDocument}
     *            to be processed.
     * @return {@link PepperMapper} object to do the mapping task for object
     *         connected to given {@link Identifier}
     */
    public PepperMapper createPepperMapper(Identifier Identifier) {
        SqueezerModuleMapper mapper = new SqueezerModuleMapper();
        return (mapper);
    }


    public static class SqueezerModuleMapper extends PepperMapperImpl implements GraphTraverseHandler {

        @Override
        public DOCUMENT_STATUS mapSCorpus() {
            return (DOCUMENT_STATUS.COMPLETED);
        }

        @Override
        public DOCUMENT_STATUS mapSDocument() {
            // set up module properties
            List<SNode> nodes = this.getDocument().getDocumentGraph().getNodes();
            for (int i = nodes.size() - 1; i >= 0; i--) {
                SNode n = nodes.get(i);
                if (n instanceof SStructure && n.getAnnotations().size() == 0) {
                    for (SRelation r : n.getInRelations()) {
                        this.getDocument().getDocumentGraph().removeRelation(r);
                    }
                    for (SRelation r : n.getOutRelations()) {
                        this.getDocument().getDocumentGraph().removeRelation(r);
                    }
                    this.getDocument().getDocumentGraph().removeNode(n);
                }
            }

            return (DOCUMENT_STATUS.COMPLETED);
        }

        @Override
        public void nodeReached(GRAPH_TRAVERSE_TYPE traversalType, String traversalId, SNode currNode, SRelation sRelation, SNode fromNode, long order) {

        }

        @Override
        public void nodeLeft(GRAPH_TRAVERSE_TYPE traversalType, String traversalId, SNode currNode, SRelation edge, SNode fromNode, long order) {
        }

        @Override
        public boolean checkConstraint(GRAPH_TRAVERSE_TYPE traversalType, String traversalId, SRelation edge, SNode currNode, long order) {
            if (currNode instanceof STextualDS) {
                return (false);
            } else {
                return (true);
            }
        }
    }

    @Override
    public boolean isReadyToStart() throws PepperModuleNotReadyException {
        return true;
    }



}
