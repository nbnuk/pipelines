package uk.org.nbn.util;

import java.util.Arrays;
import java.util.List;

public class OSGridHelpers {

  private static final List<Integer> _knownGridSizes = Arrays.asList(10, 100, 1000, 10000, 50000);

  //    public static double GridWidthToCircumradiusRounded (String input) {
  //        return String.format("") roundToDecimalPlaces(GridWidthToCircumradius(input),1);
  //    }

  /**
   * @param input a length in m representing a grid based width, this maybe a "gridSizeInMeters" or
   *     "generalisation"
   * @return the equivalent coordinateUncertaintyInMeters for a grid square of the input width
   *     (Circumradius of square)
   */
  public static String GridSizeToGridUncertainty(String input) {
    return String.format("%.1f", GridSizeToGridUncertainty(Integer.parseInt(input)));
  }

  public static double GridSizeToGridUncertainty(int input) {
    return roundToDecimalPlaces((double) input / Math.sqrt(2.0), 1);
  }

  private static double roundToDecimalPlaces(double value, int places) {
    if (places < 0) throw new IllegalArgumentException();

    long factor = (long) Math.pow(10, places);
    value = value * factor;
    long tmp = Math.round(value);
    return (double) tmp / factor;
  }
}
