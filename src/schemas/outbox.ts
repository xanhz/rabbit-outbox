import { Schema } from 'mongoose';

const OutboxSchema = new Schema(
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

OutboxSchema.index({ event: 1, createdAt: 1 });
OutboxSchema.index({ sent: 1 }, { partialFilterExpression: { sent: false } });
OutboxSchema.index({ createdAt: 1 }, { expires: '90d' }); // expires after 90 days

export { OutboxSchema };
