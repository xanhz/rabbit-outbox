import { Schema } from 'mongoose';

export const OrderSchema = new Schema(
  {
    code: {
      type: Schema.Types.String,
      required: true,
    },
  },
  {
    timestamps: {
      createdAt: 'createdAt',
      updatedAt: 'updatedAt',
    },
  }
);
