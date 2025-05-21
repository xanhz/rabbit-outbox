import { Schema } from 'mongoose';

export const OutboxSchema = new Schema(
  {
    event: {
      type: Schema.Types.String,
      index: true,
      required: true,
    },

    payload: {
      type: Schema.Types.Mixed,
    },

    sent: {
      type: Schema.Types.Boolean,
      index: true,
      default: false,
      required: true,
    },

    createdAt: {
      type: Schema.Types.Date,
      default: () => new Date(),
      required: true,
    },

    sentAt: {
      type: Schema.Types.Date,
    },
  },
  {
    timestamps: false,
  }
);
