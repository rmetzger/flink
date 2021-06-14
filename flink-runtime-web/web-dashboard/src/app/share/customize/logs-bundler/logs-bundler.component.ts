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

import {ChangeDetectionStrategy, Component, Input, OnInit} from '@angular/core';
import {flatMap, takeUntil} from "rxjs/operators";
import { Observable, Subject} from "rxjs";
import { StatusService} from "services";
import {LogsBundlerService} from "../../../services/logs-bundler.service";
import {BASE_URL} from "config";

@Component({
    selector: 'flink-logs-bundler',
    templateUrl: './logs-bundler.component.html',
    styleUrls: [],
    changeDetection: ChangeDetectionStrategy.OnPush
})
export class LogsBundlerComponent implements OnInit{
    destroy$ = new Subject();
    @Input() statusObservable: Observable<String>;
    hideSpinner: boolean = true;
    message: string = "Welcome to Flink"
    hideDownloadButton: boolean = true;

    constructor(private logBundlerService: LogsBundlerService,
                private statusService: StatusService) {
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
            //var st: string = status.toString();
            var st = status;
            console.log("st", st)

            if(st == "IDLE") {
                this.hideSpinner = true;
                this.message = "";
                this.hideDownloadButton = true;
            }
            if(st == "PROCESSING") {
                this.hideSpinner = false;
                this.message = "Please wait while bundling all logs";
                this.hideDownloadButton = true;
            }
            if (st == "BUNDLE_READY") {
                this.hideSpinner = true;
                this.message = "Bundle ready to download";
                this.hideDownloadButton = false;
                console.log(this.hideDownloadButton)
            }
            if (st == "BUNDLE_FAILED") {
                this.hideSpinner = true;
                this.message = "Creating the bundle failed";
                this.hideDownloadButton = true;
            }
            this.message = "st = " + st;
        })
    }

    requestArchive() {
        this.logBundlerService.triggerBundle();
        this.hideSpinner = false;
    }
    downloadArchive() {
        window.open(`${BASE_URL}/logbundler?action=download`);
    }

}
