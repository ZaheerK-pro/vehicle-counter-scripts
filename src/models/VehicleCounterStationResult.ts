import { Schema, model, Document } from "mongoose";

interface IVehicleTypeLog {
    vehicleType: string;
    timestamp: Date;
}

interface IVideoStream {
    day?: string;
    evening?: string;
    night?: string;
}

export interface ITotalDetected {
    "2-AXLE-TRUCK": number;
    "3-AXLE-TRUCK": number;
    "BUS": number;
    "CAR": number;
    "FARMING-EQUIPMENT": number;
    "LCV": number;
    "MULTI-AXLE-TRUCK": number;
    "THREE-WHEELER": number;
    "TWO-WHEELER": number;
}

export interface IHourlyDetected {
    hour: string;
    totalCount: number;
    vehicles: ITotalDetected;
}

interface IVehicleCounterStationResult extends Document {
    stationId: string;
    projectId: string;
    date: Date;
    vehicleTypeLogs: IVehicleTypeLog[];
    totalVehicleCount: number;
    totalDetected: ITotalDetected;
    totalHourlyDetected: IHourlyDetected[];
    videoStreams?: IVideoStream;
    isCalculated: boolean;
    totalHours?: number;
}

const vehicleTypeLogsSchema = new Schema<IVehicleTypeLog>(
    {
        vehicleType: {
            type: String,
        },
        timestamp: {
            type: Date,
        },
    },
    {
        _id: false,
    }
);

const totalDetectedSchema = new Schema(
    {
        "2-AXLE-TRUCK": {
            type: Number,
            default: 0,
        },
        "3-AXLE-TRUCK": {
            type: Number,
            default: 0,
        },
        BUS: {
            type: Number,
            default: 0,
        },
        CAR: {
            type: Number,
            default: 0,
        },
        "FARMING-EQUIPMENT": {
            type: Number,
            default: 0,
        },
        LCV: {
            type: Number,
            default: 0,
        },
        "MULTI-AXLE-TRUCK": {
            type: Number,
            default: 0,
        },
        "THREE-WHEELER": {
            type: Number,
            default: 0,
        },
        "TWO-WHEELER": {
            type: Number,
            default: 0,
        },
    },
    {
        _id: false,
    }
);

const hourlyDetectedSchema = new Schema<IHourlyDetected>(
    {
        hour: {
            type: String,
        },
        totalCount: {
            type: Number,
            default: 0,
        },
        vehicles: {
            type: totalDetectedSchema,
            default: () => ({}),
        },
    },
    {
        _id: false,
    }
);

const videoStreamSchema = new Schema<IVideoStream>(
    {
        day: {
            type: String,
        },
        evening: {
            type: String,
        },
        night: {
            type: String,
        },
    },
    {
        _id: false,
    }
);

const vehicleCounterStationResultSchema =
    new Schema<IVehicleCounterStationResult>({
        stationId: {
            type: String,
            index: true,
        },
        projectId: {
            type: String,
            index: true,
        },
        date: {
            type: Date,
            index: true,
        },
        vehicleTypeLogs: {
            type: [vehicleTypeLogsSchema],
        },
        totalVehicleCount: {
            type: Number,
            default: 0,
        },
        totalDetected: {
            type: totalDetectedSchema,
        },
        totalHourlyDetected: {
            type: [hourlyDetectedSchema],
            default: [],
        },
        isCalculated: {
            type: Boolean,
            default: false,
        },
        videoStreams: {
            type: videoStreamSchema,
            default: {},
        },
        totalHours: {
            type: Number,
            default: 0,
        },
    });

const VehicleCounterStationResultModel =
    model<IVehicleCounterStationResult>(
        "vehicleCounterStationResult",
        vehicleCounterStationResultSchema
    );

export default VehicleCounterStationResultModel;