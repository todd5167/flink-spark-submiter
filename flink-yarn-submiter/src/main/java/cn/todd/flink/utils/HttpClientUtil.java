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

package cn.todd.flink.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Date: 2020/6/15
 *
 * @author todd5167
 */
public class HttpClientUtil {
    private static int maxTotal = 100;
    private static int maxPerRoute = 20;
    private static int SocketTimeout = 5000;
    private static int ConnectTimeout = 5000;

    private static CloseableHttpClient httpClient = getHttpClient();
    private static Charset charset = Charset.forName("UTF-8");

    private static CloseableHttpClient getHttpClient() {
        ConnectionSocketFactory plainsf = PlainConnectionSocketFactory.getSocketFactory();
        LayeredConnectionSocketFactory sslsf = SSLConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> registry =
                RegistryBuilder.<ConnectionSocketFactory>create()
                        .register("http", plainsf)
                        .register("https", sslsf)
                        .build();
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(registry);
        cm.setMaxTotal(maxTotal);
        cm.setDefaultMaxPerRoute(maxPerRoute);

        // 设置请求和传输超时时间
        RequestConfig requestConfig =
                RequestConfig.custom()
                        // setConnectionRequestTimeout：设置从connect Manager获取Connection
                        // 超时时间，单位毫秒。这个属性是新加的属性，因为目前版本是可以共享连接池的。
                        .setConnectionRequestTimeout(ConnectTimeout)
                        // setSocketTimeout：请求获取数据的超时时间，单位毫秒。 如果访问一个接口，多少时间内无法返回数据，就直接放弃此次调用。
                        .setSocketTimeout(SocketTimeout)
                        // setConnectTimeout：设置连接超时时间，单位毫秒。
                        .setConnectTimeout(ConnectTimeout)
                        .build();

        return HttpClients.custom()
                .setDefaultRequestConfig(requestConfig)
                .setConnectionManager(cm)
                .build();
    }

    public static String getRequest(String url) throws IOException {
        String respBody = null;
        HttpGet httpGet = null;
        CloseableHttpResponse response = null;
        try {
            httpGet = new HttpGet(url);
            response = httpClient.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                respBody = EntityUtils.toString(entity, charset);
            } else {

                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                    throw new RuntimeException("404 error");
                } else if (response.getStatusLine().getStatusCode()
                        == HttpStatus.SC_INTERNAL_SERVER_ERROR) {
                    throw new RuntimeException("500");
                }
            }
        } catch (IOException e) {
            throw e;
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                }
            }
        }
        return respBody;
    }
}
