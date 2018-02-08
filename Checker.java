import javax.json.JsonObject;

/**
 * Created by Yuqing Wang on 2017/4/18.
 */
public class Checker {
    /**
     * this method checks whether the JsonArray is valid.
     * @param object the parent object
     * @param arrayName the name of the JsonArray in the Json string
     * @throws NullPointerException when the array is either empty or null
     */
    public static void checkJsonArray(JsonObject object, String arrayName) throws NullPointerException {
        if (object.getJsonArray(arrayName).isEmpty() || object.getJsonArray(arrayName).equals(null)) {
            throw new NullPointerException();
        }
    }

    /**
     * this method checks whether the JsonString is valid.
     * @param object the parent object
     * @param fieldName the name of that JsonString field
     * @throws NullPointerException when the JsonString is empty
     */
    public static void checkJsonString(JsonObject object, String fieldName) throws NullPointerException {
        if (object.getString(fieldName).isEmpty()) {
            throw new NullPointerException();
        }
    }

    /**
     * this method checks whether the JsonInt is valid (not negative).
     * @param object the parent object
     * @param fieldName the name of that JsonInt field
     * @throws NullPointerException when the JsonInt is negative
     */
    public static void checkJsonInt(JsonObject object, String fieldName) throws NullPointerException {
        if (object.getInt(fieldName) < 0) {
            throw new NullPointerException();
        }
    }
}
