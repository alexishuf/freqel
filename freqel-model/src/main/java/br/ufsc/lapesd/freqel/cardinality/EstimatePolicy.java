package br.ufsc.lapesd.freqel.cardinality;

public class EstimatePolicy {
    private EstimatePolicy() {}

    private static final int LIMIT_BITS = 10;

    public static final int CAN_QUERY_LOCAL  = 1 <<  LIMIT_BITS   ;
    public static final int CAN_ASK_LOCAL    = 1 << (LIMIT_BITS+1);
    public static final int CAN_QUERY_REMOTE = 1 << (LIMIT_BITS+2);
    public static final int CAN_ASK_REMOTE   = 1 << (LIMIT_BITS+3);

    public static final int CAN_LOCAL    = CAN_ASK_LOCAL | CAN_QUERY_LOCAL;
    public static final int CAN_REMOTE   = CAN_ASK_REMOTE | CAN_QUERY_REMOTE;

    public static int local(int limit) {
        return Math.min(limit, (1 << LIMIT_BITS) - 1) | CAN_LOCAL;
    }

    public static int remote(int limit) {
        return Math.min(limit, (1 << LIMIT_BITS) - 1) | CAN_REMOTE;
    }

    public static boolean canQueryLocal(int f) { return (f & CAN_QUERY_LOCAL) != 0; }
    public static boolean   canAskLocal(int f) { return (f & CAN_ASK_LOCAL  ) != 0; }
    public static boolean canQueryRemote(int f) { return (f & CAN_QUERY_REMOTE) != 0; }
    public static boolean   canAskRemote(int f) { return (f & CAN_ASK_REMOTE  ) != 0; }

    public static boolean  canLocal(int f) { return canAskLocal(f) || canQueryLocal(f); }
    public static boolean canRemote(int f) { return canAskRemote(f) || canQueryRemote(f); }

    public static int limit(int f) { return f & ((1 << LIMIT_BITS)-1); }
}
