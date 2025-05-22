import fs from 'fs';
import { mongo } from 'mongoose';

export abstract class ResumeTokenManager {
  public abstract get(): Promise<mongo.ResumeToken>;
  public abstract set(token: mongo.ResumeToken): Promise<void>;

  public serialize(token: mongo.ResumeToken) {
    return JSON.stringify(token, undefined, 0);
  }

  public deserialize(raw: string | Buffer) {
    const str = Buffer.isBuffer(raw) ? raw.toString('utf8') : raw;
    return JSON.parse(str) as mongo.ResumeToken;
  }
}

export class FileResumeTokenManager extends ResumeTokenManager {
  constructor(private readonly filepath: string) {
    super();
  }

  public get() {
    return new Promise<mongo.ResumeToken>((resolve, reject) => {
      fs.readFile(this.filepath, (err, data) => {
        if (err) {
          return resolve(undefined);
        }
        return resolve(this.deserialize(data));
      });
    });
  }

  public set(token: mongo.ResumeToken) {
    return new Promise<void>((resolve, reject) => {
      fs.writeFile(this.filepath, this.serialize(token), { encoding: 'utf8' }, err => {
        return err ? reject(err) : resolve(void 0);
      });
    });
  }
}
