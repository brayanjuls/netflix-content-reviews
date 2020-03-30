from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RedditCommentToS3(BaseOperator):

    @apply_defaults
    def __init__(self,
    			 query_date,
                 client_id,              
                 client_secret,
                 sub_reddits,
                 s3_bucket,
                 user_agent="academic_comments_understanding:v1 by /u/zekeja",                                 
                 *args, **kwargs):
        super(RedditCommentToS3, self).__init__(*args, **kwargs)
        self.query_date = query_date
        self.client_id = client_id
        self.client_secret = client_secret
        self.sub_reddits = sub_reddits
        self.s3_bucket=s3_bucket        
        self.user_agent = user_agent


    def execute(self, context):
        pass
