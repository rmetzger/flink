/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {ChangeDetectionStrategy, ChangeDetectorRef, Component, Input, OnInit} from '@angular/core';
import {flatMap, takeUntil} from "rxjs/operators";
import { Observable, Subject} from "rxjs";
import { StatusService} from "services";
import {LogsBundlerService} from "../../../services/logs-bundler.service";
import {BASE_URL} from "config";
import {LogsBundlerStatus} from "../../../interfaces/logs-bundler";

@Component({
    selector: 'flink-logs-bundler',
    templateUrl: './logs-bundler.component.html',
    styleUrls: [],
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class LogsBundlerComponent implements OnInit{
    destroy$ = new Subject();
    @Input() statusObservable: Observable<LogsBundlerStatus>;
    hideSpinner: boolean = true;
    message: string = ""
    hideDownloadButton: boolean = true;

    constructor(private logBundlerService: LogsBundlerService,
                private statusService: StatusService,
                private cdr: ChangeDetectorRef) {
    }

    ngOnInit() {
        this.statusObservable = this.statusService.refresh$
            .pipe(
                takeUntil(this.destroy$),
                flatMap(() =>
                    this.logBundlerService.getStatus()
                    )
                )
        this.statusObservable.subscribe( status => {
            this.message = status.message;
            this.hideSpinner = true;
            this.hideDownloadButton = true;

            if(status.status == "PROCESSING") {
                this.hideSpinner = false;
            }
            if (status.status == "BUNDLE_READY") {
                this.hideDownloadButton = false;
            }
            this.cdr.markForCheck();
        })
    }

    requestArchive() {
        this.logBundlerService.triggerBundle();
        this.hideSpinner = false;
        this.hideDownloadButton = true;
    }
    downloadArchive() {
        window.open(`${BASE_URL}/logbundler?action=download`);
    }

}
