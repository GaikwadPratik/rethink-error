import { Changes, r, RethinkDBError, RethinkDBErrorType } from "rethinkdb-ts";
import * as rxjs from "rxjs";
import * as rxOperator from "rxjs/operators";
import Bluebird from "bluebird";

require('dotenv').config({path: '.env'})

class Mutex {
  private _lock: null | PromiseLike<any> = null;
  constructor() {
    this._lock = null;
  }
  isLocked() {
    return this._lock !== null;
  }
  _acquire() {
    let release: any;
    const lock = this._lock = new Promise(resolve => {
      release = resolve;
    });
    return () => {
      if (this._lock === lock) this._lock = null;
      release();
    };
  }
  acquireSync() {
    if (this.isLocked()) throw new Error("still locked!");
    return this._acquire();
  }
  acquireQueued() {
    const q = Promise.resolve(this._lock).then(() => release);
    const release = this._acquire(); // reserves the lock already, but it doesn't count
    return q; // as acquired until the caller gets access to `release` through `q`
  }
}

const mutext = new Mutex();

async function connectRethink() {
  await r.connectPool({ 
    silent: true,
    timeoutError: 5000,
    waitForHealthy: true,
    db: "TestRethinkErrorDB",
    user: "admin",
    password: process.env.PASSWORD as string,
    servers: [
      {
        host: "192.168.122.2",
        port: 28015
      }
    ] 
  });
}

async function main() {
  await connectRethink();
  const changeFeed = await r.table<any>("TestRethinkErrorTable")
      .changes({ includeInitial: true, squash: false })
      .run();

  const event$ = rxjs.merge(
    rxjs.fromEvent(changeFeed, "data"),
    // convert error events to errors.
    rxjs.fromEvent(changeFeed, "error").pipe(rxOperator.tap(err => { throw err; }))
  ) as rxjs.Observable<Changes<any>| RethinkDBError>;

  const subscription = event$
      .subscribe({
        next: async(changes) => {
          if (changes instanceof Error) {
            console.log({ changes }, "I should not be reached here, something horrible has happened");
            return;
          } },
          error: async(err: RethinkDBError) => {
            subscription.unsubscribe();
            if (err.type !== RethinkDBErrorType.CONNECTION) {
              console.error(err, { errorType: err.type }, "Something other than connection error received in error handler");
              return;
            }
            console.info("Database connection error received in handler, reinitializing changefeed once rethink is up");
            let isRethinkUp = false;
            while (isRethinkUp === false) {
              const release = await mutext.acquireQueued();
              try {
                await connectRethink();
                await Bluebird.delay(1000);
                console.log("Reconnected to db localhost driver");
                isRethinkUp = true;
              } catch (_err) {
                isRethinkUp = false;
              } finally {
                release();
              }
            }
            await main();
          },
          complete: () => {
            subscription.unsubscribe();
            console.log("feed completed");
          }
        });
}

main()
  .catch(err => console.error(err));