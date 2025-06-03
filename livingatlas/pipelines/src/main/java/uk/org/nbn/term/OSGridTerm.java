package uk.org.nbn.term;

import java.net.URI;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

/** Set of terms in use by seedbank */
public enum OSGridTerm implements Term {
  gridReference,
  gridSizeInMeters,
  issues,
  gridReferenceGeodeticDatum;
  private static final URI NS_URI = URI.create("http://data.nbn.org/nbn/terms/");

  OSGridTerm() {}

  public String simpleName() {
    return this.name();
  }

  @Override
  public String prefixedName() {
    return Term.super.prefixedName();
  }

  @Override
  public String qualifiedName() {
    return Term.super.qualifiedName();
  }

  @Override
  public boolean isClass() {
    return false;
  }

  public String toString() {
    return this.prefixedName();
  }

  public String prefix() {
    return "nbn";
  }

  public URI namespace() {
    return NS_URI;
  }

  public static void RegisterTerms(TermFactory termFactory) {
    termFactory.registerTerm(OSGridTerm.gridReference);
    termFactory.registerTerm(OSGridTerm.gridSizeInMeters);
  }
}
