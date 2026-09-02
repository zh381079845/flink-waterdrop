package io.github.interestinglab.waterdrop.flink.transform;

/**
 * 无第三方依赖的 Geohash 编码（WGS84 lon/lat）。
 */
public final class GeoHash {

    private static final String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";

    private GeoHash() {
    }

    public static String encode(double longitude, double latitude, int precision) {
        if (precision <= 0) {
            return null;
        }
        if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
            return null;
        }
        double[] latInterval = new double[]{-90.0, 90.0};
        double[] lonInterval = new double[]{-180.0, 180.0};
        StringBuilder geohash = new StringBuilder();
        boolean isEven = true;
        int bit = 0;
        int ch = 0;

        while (geohash.length() < precision) {
            if (isEven) {
                double mid = (lonInterval[0] + lonInterval[1]) / 2;
                if (longitude >= mid) {
                    ch |= 1 << (4 - bit);
                    lonInterval[0] = mid;
                } else {
                    lonInterval[1] = mid;
                }
            } else {
                double mid = (latInterval[0] + latInterval[1]) / 2;
                if (latitude >= mid) {
                    ch |= 1 << (4 - bit);
                    latInterval[0] = mid;
                } else {
                    latInterval[1] = mid;
                }
            }
            isEven = !isEven;
            if (bit < 4) {
                bit++;
            } else {
                geohash.append(BASE32.charAt(ch));
                bit = 0;
                ch = 0;
            }
        }
        return geohash.toString();
    }
}
