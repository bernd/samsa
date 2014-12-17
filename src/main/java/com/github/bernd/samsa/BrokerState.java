package com.github.bernd.samsa;

/**
 * Broker states are the possible state that a kafka broker can be in.
 * A broker should be only in one state at a time.
 * The expected state transition with the following defined states is:
 *
 *                +-----------+
 *                |Not Running|
 *                +-----+-----+
 *                      |
 *                      v
 *                +-----+-----+
 *                |Starting   +--+
 *                +-----+-----+  | +----+------------+
 *                      |        +>+RecoveringFrom   |
 *                      v          |UncleanShutdown  |
 * +----------+     +-----+-----+  +-------+---------+
 * |RunningAs |     |RunningAs  |            |
 * |Controller+<--->+Broker     +<-----------+
 * +----------+     +-----+-----+
 *        |              |
 *        |              v
 *        |       +-----+------------+
 *        |-----> |PendingControlled |
 *                |Shutdown          |
 *                +-----+------------+
 *                      |
 *                      v
 *               +-----+----------+
 *               |BrokerShutting  |
 *               |Down            |
 *               +-----+----------+
 *                     |
 *                     v
 *               +-----+-----+
 *               |Not Running|
 *               +-----------+
 *
 * Custom states is also allowed for cases where there are custom kafka states for different scenarios.
 */
public class BrokerState {
    public enum States {
        NOT_RUNNING((byte) 0),
        STARTING((byte) 1),
        RECOVERING_FROM_UNCLEAN_SHUTDOWN((byte) 2),
        RUNNING_AS_BROKER((byte) 3),
        RUNNING_AS_CONTROLLER((byte) 4),
        PENDING_CONTROLLED_SHUTDOWN((byte) 6),
        BROKER_SHUTTING_DOWN((byte) 7);

        private final byte state;

        private States(byte state) {
            this.state = state;
        }

        public byte getState() {
            return state;
        }
    }

    private volatile States currentState;

    public BrokerState(States state) {
        this.currentState = state;
    }

    public States getCurrentState() {
        return currentState;
    }

    public void newState(States state) {
        this.currentState = state;
    }
}
