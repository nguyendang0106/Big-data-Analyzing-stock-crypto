import threading
import queue
import time

from demo_producer import produce
from demo_processor import process
from demo_consumer import consume


def main():
    # Simple in-memory queues for demo pipeline
    q1 = queue.Queue()
    q2 = queue.Queue()

    producer_thread = threading.Thread(target=produce, args=(q1, 5.0, 3), daemon=True)
    processor_thread = threading.Thread(target=process, args=(q1, q2, 3), daemon=True)
    consumer_thread = threading.Thread(target=consume, args=(q2,), daemon=True)

    print("[Main] Starting demo pipeline threads...")
    producer_thread.start()
    processor_thread.start()
    consumer_thread.start()

    # Wait for threads to finish. Producer signals None into q1 when done;
    # processor propagates None to q2 and exits.
    while producer_thread.is_alive() or processor_thread.is_alive() or consumer_thread.is_alive():
        time.sleep(1)

    print("[Main] Demo pipeline finished.")


if __name__ == '__main__':
    main()
