/*
 *  Created for InSilico modeling for the purpose of generating phylotrees of the Influenza Strains selected for this paper/project.
 * References used are the BioJava and Foreset Cookbooks 
 *  Created by Shaun Norris, VCU Bioinformatics MS Candidate  * 
 */

package phylotrees;

/**
 *
 * @author snorris
 */
import org.forester.archaeopteryx.Archaeopteryx;
import org.forester.phylogeny.Phylogeny;
import org.forester.phylogeny.PhylogenyNode;

public class CreateTree {

    public static void main( final String[] args ) {
        // Creating a new rooted tree with two external nodes.
        final Phylogeny phy = new Phylogeny();
        final PhylogenyNode root = new PhylogenyNode();
        final PhylogenyNode d1 = new PhylogenyNode();
        final PhylogenyNode d2 = new PhylogenyNode();
        root.setName( "root" );
        d1.setName( "descendant 1" );
        d2.setName( "descendant 2" );
        root.addAsChild( d1 );
        root.addAsChild( d2 );
        phy.setRoot( root );
        phy.setRooted( true );
        // Displaying the newly created tree with Archaeopteryx.
        Archaeopteryx.createApplication( phy );
    }
}