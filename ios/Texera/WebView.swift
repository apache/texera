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
import WebKit

@MainActor
final class WebViewModel: ObservableObject {
    @Published var canGoBack: Bool = false
    @Published var canGoForward: Bool = false
    @Published var isLoading: Bool = false
    @Published var lastError: String?

    fileprivate weak var webView: WKWebView?

    func reload() { webView?.reload() }
    func goBack() { webView?.goBack() }
    func goForward() { webView?.goForward() }
}

struct WebView: UIViewRepresentable {
    let url: URL
    @ObservedObject var model: WebViewModel

    func makeCoordinator() -> Coordinator {
        Coordinator(model: model)
    }

    func makeUIView(context: Context) -> WKWebView {
        let config = WKWebViewConfiguration()
        config.allowsInlineMediaPlayback = true
        let prefs = WKWebpagePreferences()
        prefs.allowsContentJavaScript = true
        config.defaultWebpagePreferences = prefs

        let webView = WKWebView(frame: .zero, configuration: config)
        webView.allowsBackForwardNavigationGestures = true
        webView.navigationDelegate = context.coordinator
        webView.uiDelegate = context.coordinator
        webView.scrollView.refreshControl = context.coordinator.makeRefreshControl(for: webView)
        webView.isInspectable = true

        context.coordinator.observe(webView: webView)
        context.coordinator.currentURL = url
        model.webView = webView
        webView.load(URLRequest(url: url))
        return webView
    }

    func updateUIView(_ webView: WKWebView, context: Context) {
        if context.coordinator.currentURL != url {
            context.coordinator.currentURL = url
            webView.load(URLRequest(url: url))
        }
    }

    static func dismantleUIView(_ webView: WKWebView, coordinator: Coordinator) {
        coordinator.invalidate()
    }

    @MainActor
    final class Coordinator: NSObject, WKNavigationDelegate, WKUIDelegate {
        let model: WebViewModel
        var currentURL: URL?
        private var observations: [NSKeyValueObservation] = []

        init(model: WebViewModel) {
            self.model = model
        }

        func observe(webView: WKWebView) {
            observations = [
                webView.observe(\.canGoBack, options: [.initial, .new]) { [weak self] _, change in
                    let value = change.newValue ?? false
                    DispatchQueue.main.async { self?.model.canGoBack = value }
                },
                webView.observe(\.canGoForward, options: [.initial, .new]) { [weak self] _, change in
                    let value = change.newValue ?? false
                    DispatchQueue.main.async { self?.model.canGoForward = value }
                },
                webView.observe(\.isLoading, options: [.initial, .new]) { [weak self] _, change in
                    let value = change.newValue ?? false
                    DispatchQueue.main.async { self?.model.isLoading = value }
                },
            ]
        }

        func invalidate() {
            observations.removeAll()
        }

        func makeRefreshControl(for webView: WKWebView) -> UIRefreshControl {
            let control = UIRefreshControl()
            control.addAction(UIAction { [weak webView, weak control] _ in
                webView?.reload()
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.4) {
                    control?.endRefreshing()
                }
            }, for: .valueChanged)
            return control
        }

        // MARK: WKUIDelegate

        // Route `target="_blank"` opens into the same web view rather than dropping them.
        func webView(_ webView: WKWebView,
                     createWebViewWith configuration: WKWebViewConfiguration,
                     for navigationAction: WKNavigationAction,
                     windowFeatures: WKWindowFeatures) -> WKWebView? {
            if let url = navigationAction.request.url {
                webView.load(URLRequest(url: url))
            }
            return nil
        }

        // MARK: WKNavigationDelegate

        func webView(_ webView: WKWebView, didStartProvisionalNavigation navigation: WKNavigation!) {
            model.lastError = nil
        }

        func webView(_ webView: WKWebView,
                     didFailProvisionalNavigation navigation: WKNavigation!,
                     withError error: Error) {
            handle(error: error)
        }

        func webView(_ webView: WKWebView,
                     didFail navigation: WKNavigation!,
                     withError error: Error) {
            handle(error: error)
        }

        private func handle(error: Error) {
            // Code -999 is "operation cancelled" — fires on rapid reloads / new loads, not a real failure.
            let nsError = error as NSError
            if nsError.domain == NSURLErrorDomain && nsError.code == NSURLErrorCancelled {
                return
            }
            model.lastError = error.localizedDescription
        }
    }
}
