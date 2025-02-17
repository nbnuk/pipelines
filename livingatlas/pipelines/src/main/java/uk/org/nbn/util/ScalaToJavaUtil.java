package uk.org.nbn.util;

import scala.Option;

public class ScalaToJavaUtil {
  public static String scalaOptionToString(Option<String> option) {
    if (option.isDefined()) {
      return option.get();
    } else {
      return null;
    }
  }

  public static Integer scalaOptionToJavaInteger(Option<?> scalaOption) {
    if (scalaOption.isDefined()) {
      return (Integer) scalaOption.get(); // Explicit cast to Integer
    }
    return null;
  }

  public static GISPoint scalaOptionToJavaGISPoint(Option<?> scalaOption) {
    if (scalaOption.isDefined()) {
      return (GISPoint) scalaOption.get(); // Explicit cast to Integer
    }
    return null;
  }
}
