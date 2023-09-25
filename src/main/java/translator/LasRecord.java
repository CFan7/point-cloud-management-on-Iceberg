package translator;

import java.util.Objects;

public class LasRecord {
    public double x;
    public double y;
    public double z;
    public int intensity;
    public int returnNumber;
    public int numberOfReturns;
    public int scanDirectionFlag;
    public int edgeOfFlightLine;
    public int classification;
    public int scanAngleRank;
    public int userData;
    public int pointSourceId;
    public double gpsTime;
    public int red;
    public int green;
    public int blue;

    public LasRecord(double x, double y, double z, int intensity, int returnNumber, int numberOfReturns,
                           int scanDirectionFlag, int edgeOfFlightLine, int classification, int scanAngleRank,
                          int userData, int pointSourceId, double gpsTime, int red, int green, int blue) {
        this.x = x;
        this.y = y;
        this.z = z;
        this.intensity = intensity;
        this.returnNumber = returnNumber;
        this.numberOfReturns = numberOfReturns;
        this.scanDirectionFlag = scanDirectionFlag;
        this.edgeOfFlightLine = edgeOfFlightLine;
        this.classification = classification;
        this.scanAngleRank = scanAngleRank;
        this.userData = userData;
        this.pointSourceId = pointSourceId;
        this.gpsTime = gpsTime;
        this.red = red;
        this.green = green;
        this.blue = blue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LasRecord lasRecord = (LasRecord) o;
        return Double.compare(lasRecord.x, x) == 0 &&
                Double.compare(lasRecord.y, y) == 0 &&
                Double.compare(lasRecord.z, z) == 0 &&
                intensity == lasRecord.intensity &&
                returnNumber == lasRecord.returnNumber &&
                numberOfReturns == lasRecord.numberOfReturns &&
                scanDirectionFlag == lasRecord.scanDirectionFlag &&
                edgeOfFlightLine == lasRecord.edgeOfFlightLine &&
                classification == lasRecord.classification &&
                scanAngleRank == lasRecord.scanAngleRank &&
                userData == lasRecord.userData &&
                pointSourceId == lasRecord.pointSourceId &&
                Double.compare(lasRecord.gpsTime, gpsTime) == 0 &&
                red == lasRecord.red &&
                green == lasRecord.green &&
                blue == lasRecord.blue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, z, intensity, returnNumber, numberOfReturns, scanDirectionFlag, edgeOfFlightLine,
                classification, scanAngleRank, userData, pointSourceId, gpsTime, red, green, blue);
    }
}
