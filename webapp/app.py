# app.py
from flask import Flask, render_template, request, redirect, session, url_for
from elasticsearch import Elasticsearch
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
# 引入其他模块
import elastic_utils, search_logic, recommend_logic
from flask import jsonify

app = Flask(__name__)
#app.secret_key = ''

#初始化Elasticsearch客户端
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "=j2jb70ynctOB8fIaOdi"),
    verify_certs=False
)

# 主页面
@app.route('/')
def home():
    return redirect(url_for('search'))

# 注册页面，用来注册新用户
@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        email = request.form['email']
        elastic_utils.create_user(es, username, password, email)
        return redirect(url_for('login'))
    return render_template('register.html')

# 登录页面，用来验证用户登录
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if elastic_utils.authenticate_user(es, username, password):
            session['username'] = username
            # 登录成功，转到搜索框
            return redirect(url_for('search'))
        else:
            # 登陆失败，还是这个页面，显示“登录失败”
            return render_template('login.html', error="登录失败")
    return render_template('login.html')

# 退出登录
@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))

# 搜索页面
@app.route('/search', methods=['GET', 'POST'])
def search():
    results = []
    recommended = []
    if request.method == 'POST':
        query = request.form['query']
        site = request.form.get('site')
        exact = 'exact' in request.form
        wildcard = 'wildcard' in request.form
        search_type = request.form.get('type', 'web')

        index = 'web_search_engine' if search_type == 'web' else 'web_search_engine_document'

        username = session.get('username')
        # 在这里调用了 search_logic.advanced_search 函数
        results = search_logic.advanced_search(es, index, query, 10, site, exact, wildcard,username)

        if 'username' in session:
            elastic_utils.log_query(es, session['username'], query, search_type, site, len(results))
        # 推荐逻辑的实现
        if username:
            recommended = recommend_logic.recommend_for_user(es, username)


    return render_template('search.html', results=results, user=session.get('username'),recommended=recommended)

# 历史记录页面
@app.route('/history')
def history():
    if 'username' not in session:
        return redirect(url_for('login'))
    history = elastic_utils.get_user_history(es, session['username'])
    return render_template('history.html', history=history, user=session['username'])


@app.route("/api/recent_queries")
def recent_queries():
    if 'username' not in session:
        return jsonify([])
    try:
        history = elastic_utils.get_user_history(es, session['username'])
        #获取去重后最后5条
        keywords = []
        seen = set()
        for h in reversed(history):
            if h not in seen:
                keywords.append(h)
                seen.add(h)
            if len(keywords) >= 5:
                break
        return jsonify(keywords)
    except:
        return jsonify([])



if __name__ == '__main__':
    app.run(debug=True)
