import { TimingInformation as TimingInfo } from "aws-sdk/clients/qldbsession";
import {TimingInformation} from "./TimingInformation";

export class TimingInformationImp implements TimingInformation {
    private _processingTimeMilliseconds: number;

    constructor(processingTimeMilliseconds: number) {
        this._processingTimeMilliseconds = processingTimeMilliseconds;
    }

    getProcessingTimeMilliseconds(): number {
        return this._processingTimeMilliseconds;
    }

    private _setProcessingTimeMilliseconds(value: number) {
        this._processingTimeMilliseconds = value;
    }

    accumulateTimingInfo(timingInfo: TimingInfo) {
        if (timingInfo != null) {
            this._setProcessingTimeMilliseconds(this.getProcessingTimeMilliseconds() + timingInfo.ProcessingTimeMilliseconds);
        }
    }

}
