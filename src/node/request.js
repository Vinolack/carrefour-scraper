const axios = require('axios');
const path = require('path');
const fs = require('fs');
const toml = require('toml');

// Load config from project root
const configPath = path.resolve(__dirname, '../../config.toml');
let config;
try {
    const configFile = fs.readFileSync(configPath, 'utf-8');
    config = toml.parse(configFile);
} catch (e) {
    console.error("无法读取或解析 config.toml:", e.message);
    process.exit(1);
}
const BASE_POST_URL = `http://${config.api.cf_host}:${config.api.cf_port}/cf-clearance-scraper`;

async function bypass_cf_clearance(proxyConfig) {
    try {
        let payload;
        if (proxyConfig) {
            payload = {
                url: 'https://www.worten.pt/',
                mode: "waf-session",
                proxy: {
                    host: proxyConfig.host,
                    port: proxyConfig.port,
                    username: proxyConfig.auth ? proxyConfig.auth.username : undefined,
                    password: proxyConfig.auth ? proxyConfig.auth.password : undefined
                }
            };
        } else {
            payload = {
                url: 'https://www.worten.pt/',
                mode: "waf-session",
            };
        }
        const response = await axios.post(BASE_POST_URL, payload, {
            headers: {
                'Content-Type': 'application/json'
            },
        })
        return response.data;
    }
    catch (error) {
        let msg = error.response ? `${error.response.status} - ${error.response.statusText} : ${JSON.stringify(error.response.data)}` : error.message;
        console.log(msg);
        throw new Error(`请求异常_bypass_cf_clearance: ${msg}`);
    }
}

async function bypass_cf_turnstile(proxyConfig) {
    try {
        let payload;
        if (proxyConfig) {
            payload = {
                url: "https://klokapp.ai",
                siteKey: "0x4AAAAAABdQypM3HkDQTuaO",
                mode: "turnstile-min",
                proxy: {
                    host: proxyConfig.host,
                    port: proxyConfig.port,
                    username: proxyConfig.auth ? proxyConfig.auth.username : undefined,
                    password: proxyConfig.auth ? proxyConfig.auth.password : undefined
                }
            };
        } else {
            payload = {
                url: "https://klokapp.ai",
                siteKey: "0x4AAAAAABdQypM3HkDQTuaO",
                mode: "turnstile-min"
            };
        }
        const response = await axios.post(BASE_POST_URL, payload, {
            headers: {
                'Content-Type': 'application/json'
            },
        })
        return response.data.token;
    }
    catch (error) {
        let msg = error.response ? `${error.response.status} - ${error.response.statusText} : ${JSON.stringify(error.response.data)}` : error.message;
        console.log(msg);
        throw new Error(`请求异常_bypass_cf_clearance: ${msg}`);
    }
}

async function get_page_source(url, proxyConfig) {
    try {
        let payload;
        if (proxyConfig) {
            payload = {
                url: url,
                mode: "source",
                proxy: {
                    host: proxyConfig.host,
                    port: proxyConfig.port,
                    username: proxyConfig.auth ? proxyConfig.auth.username : undefined,
                    password: proxyConfig.auth ? proxyConfig.auth.password : undefined
                }
            };
        } else {
            payload = {
                url: url,
                mode: "source",
            };
        }
        const response = await axios.post(BASE_POST_URL, payload, {
            headers: {
                'Content-Type': 'application/json'
            },
        })
        return response.data;
    }
    catch (error) {
        let msg = error.response ? `${error.response.status} - ${error.response.statusText} : ${JSON.stringify(error.response.data)}` : error.message;
        console.log(msg);
        throw new Error(`请求异常_get_page_source: ${msg}`);
    }
}

async function tls_bypass(proxyConfig, cf_clearance) {
    let initCycleTLS;
    try {
        initCycleTLS = require('cycletls');
    } catch (e) {
        throw new Error('cycletls module is not installed; tls_bypass cannot run');
    }
    const cycleTLS = await initCycleTLS();
    const response = await cycleTLS('https://doi.org/10.1093/plcell/koaf210', {
        ja3: '772,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,23-27-65037-43-51-45-16-11-13-17513-5-18-65281-0-10-35,25497-29-23-24,0',
        userAgent: cf_clearance.headers["user-agent"],
        proxy: proxyConfig ? proxyConfig.url : undefined,
        headers: {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            'sec-ch-ua-arch': '"x86"',
            'sec-ch-ua-bitness': '"64"',
            'sec-ch-ua-full-version': '"138.0.7204.169"',
            'sec-ch-ua-full-version-list': '"Not)A;Brand";v="8.0.0.0", "Chromium";v="138.0.7204.169", "Google Chrome";v="138.0.7204.169"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"19.0.0"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'cookie': cf_clearance.cookies.map(cookie => `${cookie.name}=${cookie.value}`).join('; '),
            ...cf_clearance.headers
        }
    }, 'get');
    cycleTLS.exit().catch(err => { });

    const htmlContent = response.body;
    return htmlContent;
}

module.exports = {
    bypass_cf_clearance,
    bypass_cf_turnstile,
    tls_bypass,
    get_page_source
};