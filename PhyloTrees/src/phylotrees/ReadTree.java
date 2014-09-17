/*
 *  Created for InSilico modeling for the purpose of generating phylotrees of the Influenza Strains selected for this paper/project.
 * References used are the BioJava and Foreset Cookbooks  * 
 */  

package phylotrees;

/**
 *
 * @author Shaun Norris, VCU Bioinformatics MS Candidate. norrissw@vcu.edu
 */
import java.io.File;
import java.io.IOException;

import org.forester.archaeopteryx.Archaeopteryx;
import org.forester.io.parsers.util.ParserUtils;
import org.forester.io.parsers.PhylogenyParser;
import org.forester.phylogeny.Phylogeny;
import org.forester.phylogeny.PhylogenyMethods;

public class ReadTree {

    public static void main( final String[] args ) {
        // Reading-in of (a) tree(s) from a file.
        final File treefile = new File( "/home/snorris/Downloads/phylo.xml" );
        PhylogenyParser parser = null;
        try {
            parser = ParserUtils.createParserDependingOnFileType( treefile, true );
        }
        catch ( final IOException e ) {
            e.printStackTrace();
        }
        Phylogeny[] phys = null;
        try {
            phys = PhylogenyMethods.readPhylogenies( parser, treefile );
        }
        catch ( final IOException e ) {
            e.printStackTrace();
        }
        // Display of the tree(s) with Archaeopteryx.
        Archaeopteryx.createApplication( phys );
    }
}