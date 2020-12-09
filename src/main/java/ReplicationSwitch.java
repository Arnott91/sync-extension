public class ReplicationSwitch {
    private static boolean on;


    public static boolean isOn() {
        return on;
    }

    public static void setOn(boolean on) {
        ReplicationSwitch.on = on;
    }
}
