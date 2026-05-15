//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

import SwiftUI

struct ContentView: View {
    /// The Texera frontend the app loads. Edit here and rebuild to point at a
    /// different deployment (e.g. a LAN IP or a hosted instance).
    static let backendURL = URL(string: "http://localhost:4200")!

    @StateObject private var web = WebViewModel()

    private var errorBinding: Binding<Bool> {
        Binding(
            get: { web.lastError != nil },
            set: { if !$0 { web.lastError = nil } }
        )
    }

    var body: some View {
        ZStack(alignment: .top) {
            WebView(url: Self.backendURL, model: web)
            if web.isLoading {
                ProgressView()
                    .progressViewStyle(.linear)
                    .frame(maxWidth: .infinity)
            }
        }
        .alert("Couldn't load Texera",
               isPresented: errorBinding,
               actions: {
                   Button("Retry") {
                       web.lastError = nil
                       web.reload()
                   }
                   Button("Dismiss", role: .cancel) {
                       web.lastError = nil
                   }
               },
               message: {
                   Text(web.lastError ?? "")
               })
    }
}

#Preview {
    ContentView()
}
