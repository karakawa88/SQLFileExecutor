create table blog_entry(
    id integer primary key,
    user_id integer not null,
    content text
);
create index blog_entry_user_id_index 
    on blog_entry(user_id);

